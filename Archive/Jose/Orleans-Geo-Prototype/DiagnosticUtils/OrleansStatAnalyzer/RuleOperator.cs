using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public abstract class RuleOperator
    {
        public abstract bool operate(double val, double expectedVal);
        public abstract bool operateforpercentage(double val, double expectedPercentage, double statValue);
    }

    public class RuleEqualOperator : RuleOperator
    {
        public override bool operate(double val, double expectedVal)
        {
            return (val == expectedVal);
        }

        public override bool operateforpercentage(double val, double expectedPercentage, double statValue)
        {
            if (statValue != 0)
            {
                double diff = Math.Abs(val - statValue);
                double percentage = diff/statValue;
                return (percentage == expectedPercentage);
            }
            else
            {
                return false;
            }
        }
    }

    public class RuleGraterThanOperator : RuleOperator
    {
        public override bool operate(double val, double expectedVal)
        {
            return (val > expectedVal);
        }

        public override bool operateforpercentage(double val, double expectedPercentage, double statValue)
        {
            if (statValue != 0)
            {
                double diff = Math.Abs(val - statValue);
                double percentage = diff/statValue;
                return (percentage > expectedPercentage);
            }
            else
            {
                return false;
            }
        }
    }

    public class RuleLessThanOperator : RuleOperator
    {
        public override bool operate(double val, double expectedVal)
        {
            return (val < expectedVal);
        }

        public override bool operateforpercentage(double val, double expectedPercentage, double statValue)
        {
            if (statValue != 0)
            {
                double diff = Math.Abs(val - statValue);
                double percentage = diff/statValue;
                return (percentage < expectedPercentage);
            }
            else
            {
                return false;
            }
        }
    }
}
