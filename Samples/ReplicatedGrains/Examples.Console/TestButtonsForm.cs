using Examples.Interfaces;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Examples.Test
{
    public partial class TestButtonsForm : Form
    {
        public TestButtonsForm()
        {
            InitializeComponent();
        }


        IAccountGrain account;
        TaskScheduler uischeduler;

        private void TestButtonsForm_Load(object sender, EventArgs e)
        {
            account = Examples.Interfaces.AccountGrainFactory.GetGrain(0);

            textBox3_TextChanged(null, null);
            textBox4_TextChanged(null, null);
            textBox5_TextChanged(null, null);

            uischeduler = TaskScheduler.FromCurrentSynchronizationContext();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            label1.Text = "(pending)";
            var bgtask = ReadAccount();
        }

        private Task ReadAccount()
        {
            return account.EstimatedBalance().ContinueWith(t =>
            {
                label1.Text = t.IsFaulted ? "ERROR" : t.Result.ToString();
            },
            uischeduler);
        }

        private void button2_Click(object sender, EventArgs e)
        {
            uint amount;
            if (!uint.TryParse(textBox1.Text, out amount))
            {
                label2.Text = "invalid";
                return;
            }
            label2.Text = "(pending)";
            account.UnreliableDeposit(amount).ContinueWith(t =>
            {
                label2.Text = t.IsFaulted ? "ERROR" : "ok";
            },
            uischeduler);
        }

        private void button3_Click(object sender, EventArgs e)
        {
            uint amount;
            if (!uint.TryParse(textBox2.Text, out amount))
            {
                label3.Text = "invalid";
                return;
            }
            label3.Text = "(pending)";
            account.Withdraw(amount).ContinueWith(t =>
            {
                label3.Text = t.IsFaulted ? "ERROR" : (t.Result ? "ok" : "nope");
            },
            uischeduler);
        }

        private void groupBox2_Enter(object sender, EventArgs e)
        {

        }

        private void label6_Click(object sender, EventArgs e)
        {

        }

        private void textBox3_TextChanged(object sender, EventArgs e)
        {
            uint delay;
            if (uint.TryParse(textBox3.Text, out delay))
                Examples.Interfaces.DebuggingControlsFactory.GetGrain(0).SetArtificialReadDelay(delay);
        }

        private void textBox4_TextChanged(object sender, EventArgs e)
        {
            uint delay;
            if (uint.TryParse(textBox4.Text, out delay))
                Examples.Interfaces.DebuggingControlsFactory.GetGrain(0).SetArtificialWriteDelay(delay);
        }

        private void label7_Click(object sender, EventArgs e)
        {

        }

        private void textBox5_TextChanged(object sender, EventArgs e)
        {
            uint bound;
            if (uint.TryParse(textBox5.Text, out bound))
                Examples.Interfaces.DebuggingControlsFactory.GetGrain(0).SetStalenessBound(bound);
        }


        private Task autoreader;
        private volatile bool terminationflag;

        private void checkBox1_CheckedChanged(object sender, EventArgs e)
        {
            if (checkBox1.Checked)
            {
                button1.Enabled = false;
                autoreader = Task.Factory.StartNew(() => {
                    while (!terminationflag)
                        ReadAccount().Wait(); // read in loop
                });
            }
            else
            {
                terminationflag = true;
                checkBox1.Enabled = false;
                autoreader.ContinueWith((o) =>
                {   autoreader = null;
                    terminationflag = false;
                    button1.Enabled = true;
                    checkBox1.Enabled = true;
                }, uischeduler);
            }
        }

    }
}
