using Microsoft.WindowsAzure.Storage.Pileus;
using PileusApp.Utils;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using TechFestDemo;
using System.Windows.Forms.DataVisualization.Charting;
using System.Threading;
using PileusApp.YCSB;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;


namespace TechFestDemoApplication
{
    public partial class Form1 : Form
    {
        bool enableHitRate = true;

        Sampler initialSampler = null;
        Sampler reconfigSampler = null;
        bool initialConfig = true;

        List<string> consistencyChoices;
        List<string> readLatency;
        List<string> readAgainLatency;
        bool readAgain = false;

        BackgroundWorker readWriteWorker;

        public Form1()
        {
            InitializeComponent();

            consistencyChoices = new List<string> { 
                "strong consistency", 
                "causal consistency", 
                "bounded staleness", 
                "read my writes", 
                "monotonic freshness", 
                "eventual consistency" 
            };
            consistencyListBox.DataSource = consistencyChoices;
            consistencyListBox.ClearSelected();

            readWriteWorker = new BackgroundWorker();
            readWriteWorker.DoWork += new DoWorkEventHandler(ReadWriteWorker_DoWork);
            readWriteWorker.RunWorkerCompleted += new RunWorkerCompletedEventHandler(ReadWriteWorker_Completed);

            Print("Running demo program...");
            DemoLib.RegisterLogger(Print);

            //
            // Create initial configuration
            //
            Print("Reading configuration...");
            DemoLib.Initialize();
            Print(DemoLib.PrintCurrentConfiguration());

            // Read sample data from file
            Print("Reading file data...");
            initialSampler = DemoLib.NewSampler();
            reconfigSampler = DemoLib.NewSampler();
            DemoLib.ReadDataFile(initialSampler);
            Print(DemoLib.PrintReadWriteTimes(initialSampler));

            // Ping servers to measure and record round-trip times in the server state
            Print("Pinging servers...");
            DemoLib.PingAllServers();
            Print("Round-trip latencies to sites:");
            Print(DemoLib.PrintServerRTTs());
        }

        delegate void PrintCallback(string text);

        private void Print(string s)
        {
            if (this.logTextBox.InvokeRequired)
            {
                PrintCallback d = new PrintCallback(Print);
                this.Invoke(d, new object[] { s });
            }
            else
            {
                logTextBox.AppendText(s);
                logTextBox.AppendText("\r\n");
            }
        }

        private void ReadButton_Click(object sender, EventArgs e)
        {
            if (!readWriteWorker.IsBusy)
            {
                Print("Reading and writing blobs...");
                readAgain = false;
                readWriteWorker.RunWorkerAsync();
            }
        }

        void ReadWriteWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            Sampler sampler = initialConfig ? initialSampler : reconfigSampler;
            sampler = DemoLib.PerformReadsWritesSyncs(sampler, (initialConfig == false));
            if (initialConfig)
                initialSampler = sampler;
            else
                reconfigSampler = sampler;
        }

        void ReadWriteWorker_Completed(object sender, RunWorkerCompletedEventArgs e)
        {
            Print("Read and write average latencies:");
            Sampler sampler = initialConfig ? initialSampler : reconfigSampler;
            Print(DemoLib.PrintReadWriteTimes(sampler));

            readLatency = new List<string>();
            readLatency.Add(sampler.GetSampleValue("strongLatency").ToString("F2").PadLeft(8));
            readLatency.Add(sampler.GetSampleValue("causalLatency").ToString("F2").PadLeft(8));
            readLatency.Add(sampler.GetSampleValue("boundedLatency").ToString("F2").PadLeft(8));
            readLatency.Add(sampler.GetSampleValue("readmywritesLatency").ToString("F2").PadLeft(8));
            readLatency.Add(sampler.GetSampleValue("monotonicLatency").ToString("F2").PadLeft(8));
            readLatency.Add(sampler.GetSampleValue("eventualLatency").ToString("F2").PadLeft(8));

            if (!readAgain)
            {
                readLatencyListBox.DataSource = null;
                readLatencyListBox.DataSource = readLatency;
                readLatencyListBox.ClearSelected();
            }
            else
            {
                readAgainLatencyListBox.DataSource = null;
                readAgainLatencyListBox.DataSource = readLatency;
                readAgainLatencyListBox.ClearSelected();
            }
            
            if (initialConfig)
            {
                DemoLib.WriteDataFile(sampler);
            }
        }

        private void ReadAgainButton_Click(object sender, EventArgs e)
        {
            if (!readWriteWorker.IsBusy)
            {
                Print("Reading and writing blobs...");
                readAgain = true;
                readWriteWorker.RunWorkerAsync();
            }
        }

        private void ClearButton_Click(object sender, EventArgs e)
        {
            readLatencyListBox.DataSource = null;
            readAgainLatencyListBox.DataSource = null;
            consistencyListBox.ClearSelected();
        }

        private void getConfigButton_Click(object sender, EventArgs e)
        {
            configTextBox.Clear();
            configTextBox.AppendText(DemoLib.PrintCurrentConfiguration() + "\n");
        }

        private void restroreConfigButton_Click(object sender, EventArgs e)
        {
            configTextBox.Clear();
            configTextBox.AppendText("Restoring initial configuration...");
            DemoLib.SetInitialConfiguration();
            initialConfig = true;
            configTextBox.Clear();
            configTextBox.AppendText(DemoLib.PrintCurrentConfiguration() + "\n");
        }

        private void proposeNewConfigButton_Click(object sender, EventArgs e)
        {
            configTextBox.Clear();
            configTextBox.AppendText("Proposing new configuration...");
            DemoLib.ProposeNewConfiguration();
            configTextBox.Clear();
            configTextBox.AppendText(DemoLib.PrintReconfigurationActions() + "\n");
        }

        private void installNewConfigButton_Click(object sender, EventArgs e)
        {
            configTextBox.Clear();
            configTextBox.AppendText("Installing new configuration...");
            DemoLib.InstallNewConfiguration();
            initialConfig = false;
            reconfigSampler = DemoLib.NewSampler();
            configTextBox.Clear();
            configTextBox.AppendText(DemoLib.PrintCurrentConfiguration() + "\n");
        }

        private void installSLA2Button_Click(object sender, EventArgs e)
        {
            DemoLib.SetCurrentSLA("Shopping Cart");
            ServiceLevelAgreement sla = DemoLib.GetCurrentSLA();
            DisplaySLA(sla);
        }

        private void installSLA1Button_Click(object sender, EventArgs e)
        {
            DemoLib.SetCurrentSLA("Fast or Strong");
            ServiceLevelAgreement sla = DemoLib.GetCurrentSLA();
            DisplaySLA(sla);
        }

        private void getCurrentSLAButton_Click(object sender, EventArgs e)
        {
            ServiceLevelAgreement sla = DemoLib.GetCurrentSLA();
            DisplaySLA(sla);
        }

        private void DisplaySLA(ServiceLevelAgreement sla) 
        {
            List<string> slaConsistency = new List<string>();
            List<string> slaLatency = new List<string>();
            List<string> slaUtility = new List<string>();
            foreach (SubSLA sub in sla)
            {
                string consistency = sub.Consistency.ToString();
                if (sub.Bound != 0)
                {
                    consistency += " (" + sub.Bound.ToString() + " sec)";
                }
                slaConsistency.Add(consistency);
                slaLatency.Add(sub.Latency.ToString() + " ms.");
                slaUtility.Add(sub.Utility.ToString());
            }
            slaConsistencyListBox.DataSource = null;
            slaConsistencyListBox.DataSource = slaConsistency;
            slaConsistencyListBox.ClearSelected();
            slaLatencyListBox.DataSource = null;
            slaLatencyListBox.DataSource = slaLatency;
            slaLatencyListBox.ClearSelected();
            slaUtilityListBox.DataSource = null;
            slaUtilityListBox.DataSource = slaUtility;
            slaUtilityListBox.ClearSelected();
            
            Sampler sampler = initialConfig ? initialSampler : reconfigSampler;
            slaDeliveredUtility.Text = DemoLib.GetCurrentSLAUtility().ToString("F2");
            slaReadTime.Text = sampler.GetSampleValue("slaLatency").ToString("F2");
        }

        // Load Chart button
        private void button1_Click(object sender, EventArgs e)
        {
            Sampler sampler = initialConfig ? initialSampler : reconfigSampler;
            /*
            sampler = DemoLib.PerformReadsWritesSyncs(sampler);
            if (initialConfig)
                initialSampler = sampler;
            else
                reconfigSampler = sampler;
             */

            this.chart1.Series["Latency"].Points.Clear();
            this.chart1.Series["Primary hit rate"].Points.Clear(); 
            this.chart1.Legends["Latency"].Enabled = true;

            if (enableHitRate)
            {
                this.chart1.Series["Primary hit rate"].IsVisibleInLegend = true;
            }
            else
            {
                this.chart1.Series["Primary hit rate"].IsVisibleInLegend = false;
            }

            this.chart1.Series["Latency"].Points.AddXY("Strong", sampler.GetSampleValue("strongLatency"));
            if (enableHitRate)
            {
                float primaryHitRate = (sampler.GetSampleValue("strongPrimaryAccesses") * 100) / sampler.GetSampleValue("strongTotalAccesses");
                this.chart1.Series["Primary hit rate"].Points.AddXY("Strong", primaryHitRate);
            }

            this.chart1.Series["Latency"].Points.AddXY("Causal", sampler.GetSampleValue("causalLatency"));
            if (enableHitRate)
            {
                float primaryHitRate = (sampler.GetSampleValue("causalPrimaryAccesses") * 100) / sampler.GetSampleValue("causalTotalAccesses");
                this.chart1.Series["Primary hit rate"].Points.AddXY("Causal", primaryHitRate);
            }

            this.chart1.Series["Latency"].Points.AddXY("Bounded", sampler.GetSampleValue("boundedLatency"));
            if (enableHitRate)
            {
                float primaryHitRate = (sampler.GetSampleValue("boundedPrimaryAccesses") * 100) / sampler.GetSampleValue("boundedTotalAccesses");
                this.chart1.Series["Primary hit rate"].Points.AddXY("Bounded", primaryHitRate);
            }

            this.chart1.Series["Latency"].Points.AddXY("Read my writes", sampler.GetSampleValue("readmywritesLatency"));
            if (enableHitRate)
            {
                float primaryHitRate = (sampler.GetSampleValue("readmywritesPrimaryAccesses") * 100) / sampler.GetSampleValue("readmywritesTotalAccesses");
                this.chart1.Series["Primary hit rate"].Points.AddXY("Read my writes", primaryHitRate);
            }

            this.chart1.Series["Latency"].Points.AddXY("Monotonic", sampler.GetSampleValue("monotonicLatency"));
            if (enableHitRate)
            {
                float primaryHitRate = (sampler.GetSampleValue("monotonicPrimaryAccesses") * 100) / sampler.GetSampleValue("monotonicTotalAccesses");
                this.chart1.Series["Primary hit rate"].Points.AddXY("Monotonic", primaryHitRate);
            }

            this.chart1.Series["Latency"].Points.AddXY("Eventual", sampler.GetSampleValue("eventualLatency"));
            if (enableHitRate)
            {
                float primaryHitRate = (sampler.GetSampleValue("eventualPrimaryAccesses") * 100) / sampler.GetSampleValue("eventualTotalAccesses");
                this.chart1.Series["Primary hit rate"].Points.AddXY("Eventual", primaryHitRate);
            }
        }

        public void DrawChart()
        {
            while (true)
            {
                this.Invoke(new Action(() => { buttonLoadChart.PerformClick(); }));
                Thread.Sleep(100);
            }
        }

        // Chart Refresh button
        private void button2_Click(object sender, EventArgs e)
        {
           Thread workedThread = new Thread(DrawChart);
            workedThread.Start();
            button2.Enabled = false;
        }

        private void chart1_Click(object sender, EventArgs e)
        {
            if (this.enableHitRate)
            {
                enableHitRate = false;
            }
            else
            {
                enableHitRate = true;
            }
        }

        private void getReplicasButton_Click(object sender, EventArgs e)
        {
            ReplicaConfiguration config = DemoLib.GetCurrentConfiguration();
            string server = DemoLib.ServerName("West US");
            if (config.PrimaryServers.Contains(server))
                radioButtonWestUSPrimary.Checked = true;
            else if (config.SecondaryServers.Contains(server))
                radioButtonWestUSSecondary.Checked = true;
            else
                radioButtonWestUSUnused.Checked = true;
            if (config.ReadOnlySecondaryServers.Contains(server))
                radioButtonWestUSPrimary.Enabled = false;
            server = DemoLib.ServerName("East US");
            if (config.PrimaryServers.Contains(server))
                radioButtonEastUSPrimary.Checked = true;
            else if (config.SecondaryServers.Contains(server))
                radioButtonEastUSSecondary.Checked = true;
            else
                radioButtonEastUSUnused.Checked = true;
            if (config.ReadOnlySecondaryServers.Contains(server))
                radioButtonEastUSPrimary.Enabled = false;
            server = DemoLib.ServerName("South US");
            if (config.PrimaryServers.Contains(server))
                radioButtonSouthUSPrimary.Checked = true;
            else if (config.SecondaryServers.Contains(server))
                radioButtonSouthUSSecondary.Checked = true;
            else
                radioButtonSouthUSUnused.Checked = true;
            if (config.ReadOnlySecondaryServers.Contains(server))
                radioButtonSouthUSPrimary.Enabled = false;
            server = DemoLib.ServerName("North US");
            if (config.PrimaryServers.Contains(server))
                radioButtonNorthUSPrimary.Checked = true;
            else if (config.SecondaryServers.Contains(server))
                radioButtonNorthUSSecondary.Checked = true;
            else
                radioButtonNorthUSUnused.Checked = true;
            if (config.ReadOnlySecondaryServers.Contains(server))
                radioButtonNorthUSPrimary.Enabled = false;
            server = DemoLib.ServerName("West Europe");
            if (config.PrimaryServers.Contains(server))
                radioButtonWestEuropePrimary.Checked = true;
            else if (config.SecondaryServers.Contains(server))
                radioButtonWestEuropeSecondary.Checked = true;
            else
                radioButtonWestEuropeUnused.Checked = true;
            if (config.ReadOnlySecondaryServers.Contains(server))
                radioButtonWestEuropePrimary.Enabled = false;
            server = DemoLib.ServerName("North Europe");
            if (config.PrimaryServers.Contains(server))
                radioButtonNorthEuropePrimary.Checked = true;
            else if (config.SecondaryServers.Contains(server))
                radioButtonNorthEuropeSecondary.Checked = true;
            else
                radioButtonNorthEuropeUnused.Checked = true;
            if (config.ReadOnlySecondaryServers.Contains(server))
                radioButtonNorthEuropePrimary.Enabled = false;
            server = DemoLib.ServerName("East Asia");
            if (config.PrimaryServers.Contains(server))
                radioButtonAsiaPrimary.Checked = true;
            else if (config.SecondaryServers.Contains(server))
                radioButtonAsiaSecondary.Checked = true;
            else
                radioButtonAsiaUnused.Checked = true;
            if (config.ReadOnlySecondaryServers.Contains(server))
                radioButtonAsiaPrimary.Enabled = false;
            server = DemoLib.ServerName("Brazil");
            if (config.PrimaryServers.Contains(server))
                radioButtonBrazilPrimary.Checked = true;
            else if (config.SecondaryServers.Contains(server))
                radioButtonBrazilSecondary.Checked = true;
            else
                radioButtonBrazilUnused.Checked = true;
            if (config.ReadOnlySecondaryServers.Contains(server))
                radioButtonBrazilPrimary.Enabled = false;
        }

        private void setReplicasButton_Click(object sender, EventArgs e)
        {
            ReplicaConfiguration config = DemoLib.GetCurrentConfiguration();
            config.PrimaryServers.Clear();
            config.SecondaryServers.Clear();
            config.NonReplicaServers.Clear();
            string server = DemoLib.ServerName("West US");
            if (radioButtonWestUSPrimary.Checked)
                config.PrimaryServers.Add(server);
            else if (radioButtonWestUSSecondary.Checked)
                config.SecondaryServers.Add(server);
            else
                config.NonReplicaServers.Add(server);
            server = DemoLib.ServerName("East US");
            if (radioButtonEastUSPrimary.Checked)
                config.PrimaryServers.Add(server);
            else if (radioButtonEastUSSecondary.Checked)
                config.SecondaryServers.Add(server);
            else
                config.NonReplicaServers.Add(server);
            server = DemoLib.ServerName("South US");
            if (radioButtonSouthUSPrimary.Checked)
                config.PrimaryServers.Add(server);
            else if (radioButtonSouthUSSecondary.Checked)
                config.SecondaryServers.Add(server);
            else
                config.NonReplicaServers.Add(server);
            server = DemoLib.ServerName("North US");
            if (radioButtonNorthUSPrimary.Checked)
                config.PrimaryServers.Add(server);
            else if (radioButtonNorthUSSecondary.Checked)
                config.SecondaryServers.Add(server);
            else
                config.NonReplicaServers.Add(server);
            server = DemoLib.ServerName("West Europe");
            if (radioButtonWestEuropePrimary.Checked)
                config.PrimaryServers.Add(server);
            else if (radioButtonWestEuropeSecondary.Checked)
                config.SecondaryServers.Add(server);
            else
                config.NonReplicaServers.Add(server);
            server = DemoLib.ServerName("North Europe");
            if (radioButtonNorthEuropePrimary.Checked)
                config.PrimaryServers.Add(server);
            else if (radioButtonNorthEuropeSecondary.Checked)
                config.SecondaryServers.Add(server);
            else
                config.NonReplicaServers.Add(server);
            server = DemoLib.ServerName("East Asia");
            if (radioButtonAsiaPrimary.Checked)
                config.PrimaryServers.Add(server);
            else if (radioButtonAsiaSecondary.Checked)
                config.SecondaryServers.Add(server);
            else
                config.NonReplicaServers.Add(server);
            server = DemoLib.ServerName("Brazil");
            if (radioButtonBrazilPrimary.Checked)
                config.PrimaryServers.Add(server);
            else if (radioButtonBrazilSecondary.Checked)
                config.SecondaryServers.Add(server);
            else
                config.NonReplicaServers.Add(server);
            initialConfig = false;
            reconfigSampler = DemoLib.NewSampler();
            DemoLib.SetCurrentConfiguration(config);
        }

        private void buttonGetLatency_Click(object sender, EventArgs e)
        {
            ServerMonitor monitor = DemoLib.GetServerMonitor();
            ReplicaConfiguration config = DemoLib.GetCurrentConfiguration();
            List<string> allServers = config.GetServers();
            List<string> siteNames = new List<string>();
            List<string> siteLatency = new List<string>();
            
            foreach (string server in allServers)
            {
                ServerState ss = monitor.GetServerState(server);
                siteNames.Add(DemoLib.SiteName(server));
                siteLatency.Add(ss.RTTs.FindAverage().ToString("F0").PadLeft(5));
            }

            listBoxLatencySites.DataSource = null;
            listBoxLatencySites.DataSource = siteNames;
            listBoxLatencySites.ClearSelected();
            listBoxLatencyTimes.DataSource = null;
            listBoxLatencyTimes.DataSource = siteLatency;
            listBoxLatencyTimes.ClearSelected();
        }

        private void buttonPingLatency_Click(object sender, EventArgs e)
        {
            DemoLib.PingAllServers();
            buttonGetLatency_Click(sender, e);
        }

        private void buttonClearLatency_Click(object sender, EventArgs e)
        {
            listBoxLatencyTimes.DataSource = null;
            listBoxLatencySites.ClearSelected();
        }
        
    }
}
