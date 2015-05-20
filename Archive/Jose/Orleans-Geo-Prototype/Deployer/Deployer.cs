using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using SystemsManagement;

namespace Orleans.Management.Deployment
{
    public class Deployer
    {
        public Action<string> Log { get; set; }
        public Action<string> Warn { get; set; }

        private int deployInProgress;
        private int deployErrors;

        public Deployer()
        {
            this.Log = Console.WriteLine;
            this.Warn = Console.WriteLine;

            deployInProgress = 0;
            deployErrors = 0;
        }

        public void CopyDirectory(string what, TargetLocation to, DirectoryInfo from, string filter, string excludeFile)
        {
            // Note: the assumption is that this code is being called from the main ui thread, so ui updates are OK

            deployInProgress++;

            string targetLoc = to.GetUncPath();

            Log(string.Format("Copying {0}: {1} to {2}", what, from.FullName, targetLoc));

            if (filter == null) filter = "*.*";

            string otherArgs = "/E /W:2";
            if (excludeFile != null) otherArgs = otherArgs + " /XF " + excludeFile;

            ActionStatus cmd = Actions.RoboCopy(from.FullName, targetLoc, filter, otherArgs);

            if (cmd.Status)
            {
                cmd.Process.Exited += (s, e) =>
                {
                    // Note: this code will execute on a different thread from the main ui thread
                    bool ok = CheckRobocopyResult(cmd.Process.ExitCode, targetLoc);
                };

                cmd.Process.EnableRaisingEvents = true;
            }
            else
            {
                Warn("RoboCopy process failed to start correctly");
            }
        }

        public void CopyFile(string what, TargetLocation to, string fileName, FileInfo from)
        {
            // Note: the assumption is that this code is being called from the main ui thread, so ui updates are OK

            deployInProgress++;

            string targetLoc = to.GetUncPath();
            string targetFileUnc = targetLoc + @"\" + fileName;

            Log(string.Format("Copying {0}: {1} to {2}", what, from.FullName, targetFileUnc));

            try
            {
                File.Copy(from.FullName, targetFileUnc, true);
            }
            catch (Exception exc)
            {
                Warn("Failed to copy " + what + ": Error = " + exc.ToString());
            }

            ////string otherArgs = "/W:2";

            ////ActionStatus cmd = Actions.RoboCopy(from.Directory.FullName, targetLoc, from.Name, otherArgs);

            ////if (cmd.Status)
            ////{
            ////    cmd.Process.Exited += (s, e) =>
            ////    {
            ////        // Note: this code will execute on a different thread from the main ui thread
            ////        bool ok = CheckRobocopyResult(cmd.Process.ExitCode, targetLoc);
            ////    };

            ////    cmd.Process.EnableRaisingEvents = true;
            ////}
            ////else
            ////{
            ////    Warn("RoboCopy process failed to start correctly");
            ////}
        }

        public void StartProcess(TargetLocation place, string progName, string cmdlineArgs)
        {
            string startingMsg = string.Format("Executing {0} on {1} at {2} using args: {3}", progName, place.Host, place.Path, cmdlineArgs);
            Log(startingMsg);

            List<ProcessDescriptor> ProcessesToStart = new List<ProcessDescriptor>
            {
                new ProcessDescriptor { MachineName = place.Host, Path = place.Path, Name = progName, Parameters = cmdlineArgs }
            };

            Actions.CreateProcesses(ProcessesToStart);

            long rc = ProcessesToStart[0].CreateReturnCode;
            if (rc != 0)
            {
                Warn(string.Format("{0} process creation on {1} failed with code={2}", progName, place.Host, rc));
            }

            //ActionStatus cmd = Actions.RemoteExecute(place.Host, place.Path, progName, cmdlineArgs);
            //if (cmd.Status)
            //{
            //    cmd.Process.Exited += (s, e) =>
            //    {
            //        try
            //        {
            //            Log(string.Format("RemoteExecute process exited with code={0}", cmd.Process.ExitCode));
            //        }
            //        catch (Exception exc)
            //        {
            //            string errMsg = "Error during Process.Exited event notification: " + exc.ToString();
            //            try
            //            {
            //                Warn(errMsg);
            //            }
            //            catch (Exception exc2)
            //            {
            //                // Fallback to Debug logging if there is any problems with the original message logging
            //                Debug.WriteLine(errMsg);
            //                Debug.WriteLine(exc2.ToString());
            //            }
            //        }
            //    };

            //    cmd.Process.EnableRaisingEvents = true;
            //}
            //else
            //{
            //    Warn("RemoteExecute status returned false");
            //}
        }

        public void StopProcess(TargetLocation place, string progName)
        {
            string startingMsg = string.Format("Stopping {0} on host {1}", progName, place.Host);
            Log(startingMsg);

            List<ProcessDescriptor> processesToStop = new List<ProcessDescriptor>
            {
                new ProcessDescriptor { MachineName = place.Host, Name = progName }
            };

            Actions.TerminateProcesses(processesToStop);

            long rc = processesToStop[0].TerminateReturnCode;
            if (rc == 0)
            {
                Log(string.Format("{0} process was stopped on host {1}", progName, place.Host));
            }
            else if (rc == -1)
            {
                Log(string.Format("{0} process is not running on host {1}", progName, place.Host));
            }
            else 
            {
                Warn(string.Format("{0} process termination on host {1} failed with code={2}", progName, place.Host, rc));
            }
        }

        public void MonitorRobocopyCompletion(string what)
        {
            // Note: the assumption is that this code is being called from the main ui thread, so ui updates are OK

            while (deployInProgress > 0)
            {
                Log("Waiting for completion of " + deployInProgress + " deployment operation" + (deployInProgress != 1 ? "s" : ""));

                Thread.Sleep(100);
            }

            if (deployErrors > 0)
            {
                Warn("Orleans deployment completed with " + deployErrors + " error" + (deployErrors != 1 ? "s" : ""));
            }
            else
            {
                Log("Orleans deployment completed successfully");
            }
        }

        private bool CheckRobocopyResult(int exitCode, string targetLoc)
        {
            // RoboCopy exit codes from http://ss64.com/nt/robocopy-exit.html
            //
            // The return code from Robocopy is a bit map, defined as follows:
            //
            // Hex   Decimal  Meaning if set
            // 0×10  16       Serious error. Robocopy did not copy any files.
            //                Either a usage error or an error due to insufficient access privileges
            //                on the source or destination directories.
            //
            // 0×08   8       Some files or directories could not be copied
            //                (copy errors occurred and the retry limit was exceeded).
            //                Check these errors further.
            //
            // 0×04   4       Some Mismatched files or directories were detected.
            //                Examine the output log. Some housekeeping may be needed.
            //
            // 0×02   2       Some Extra files or directories were detected.
            //                Examine the output log for details. 
            //
            // 0×01   1       One or more files were copied successfully (that is, new files have arrived).
            //
            // 0×00   0       No errors occurred, and no copying was done.
            //               The source and destination directory trees are completely synchronized. 

            try
            {
                deployInProgress--;

                if (exitCode >= 0x08)
                {
                    Warn("Robocopy to " + targetLoc + " completed with ExitCode=" + exitCode);
                    if ((exitCode & 0x08) != 0)
                    {
                        Warn("Some files or directories could not be copied by Robocopy.");
                    }
                    if ((exitCode & 0x10) != 0)
                    {
                        Warn("Serious error. Robocopy did not copy any files.");
                    }
                    deployErrors++;
                    return false;
                }
                else
                {
                    Log("Robocopy to " + targetLoc + " completed successfully");
                    return true;
                }
            }
            catch (Exception exc)
            {
                string errMsg = "Error during Process.Exited event notification: " + exc.ToString();
                try {
                    Warn(errMsg);
                }
                catch (Exception exc2) {
                    // Fallback to Debug logging if there is any problems with the original message logging
                    Debug.WriteLine(errMsg);
                    Debug.WriteLine(exc2.ToString());
                }
                return false;
            }
        }

    }
}
