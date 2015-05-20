using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;

namespace Orleans.RuntimeCore.Configuration
{
    [Serializable]
    public class ApplicationConfiguration
    {
        private readonly Dictionary<string, GrainTypeConfiguration> _classSpecific;
        private GrainTypeConfiguration _defaults;

        public TimeSpan DefaultCollectionAgeLimit
        {
            get
            {
                if (_defaults.CollectionAgeLimit.HasValue)
                {
                    return _defaults.CollectionAgeLimit.Value;
                }
                else
                {
                    return Constants.DEFAULT_COLLECTION_AGE_LIMIT;
                }
            }
        }

        public ApplicationConfiguration(TimeSpan? collectionAgeLimit = null)
        {
            _classSpecific = new Dictionary<string, GrainTypeConfiguration>();
            _defaults = new GrainTypeConfiguration(null)
                        {
                            CollectionAgeLimit = collectionAgeLimit
                        };
        }

        public IEnumerable<GrainTypeConfiguration> ClassSpecific { get { return _classSpecific.Values; } }

        public void Load(XmlElement xmlElement, Logger logger)
        {
            bool found = false;
            foreach (XmlNode node in xmlElement.ChildNodes)
            {
                found = true;
                GrainTypeConfiguration config = GrainTypeConfiguration.Load((XmlElement)node, logger);
                if (null != config)
                {
                    if (config.AreDefaults)
                    {
                        _defaults = config;
                    }
                    else
                    {
                        if (_classSpecific.ContainsKey(config.Type.FullName))
                        {
                            // todo: need more informative error message here.
                            throw new InvalidOperationException(string.Format("duplicate type {0} in configuration", config.Type.FullName));
                        }
                        _classSpecific.Add(config.Type.FullName, config);
                    }
                }
            }

            if (!found)
            {
                throw new InvalidOperationException("empty GrainTypeConfiguration element");
            }
        }

        public override string ToString()
        {
            StringBuilder result = new StringBuilder();
            result.AppendFormat("   Application:\n");
            result.AppendFormat("      Defaults:\n");
            result.AppendFormat("         Deactivate if idle for: {0}\n", DefaultCollectionAgeLimit);
            foreach (GrainTypeConfiguration config in _classSpecific.Values)
            {
                if (config.CollectionAgeLimit.HasValue)
                {
                    result.AppendFormat("      GrainType Type=\"{0}\":\n", config.Type.FullName);
                    result.AppendFormat("         Deactivate if idle for: {0} sec\n", (long)config.CollectionAgeLimit.Value.TotalSeconds);
                }
            }
            return result.ToString();
        }

        public TimeSpan GetCollectionAgeLimit(Type type)
        {
            return GetCollectionAgeLimit(type.FullName);
        }

        public TimeSpan GetCollectionAgeLimit(string fullName)
        {
            GrainTypeConfiguration config;
            if (_classSpecific.TryGetValue(fullName, out config) && config.CollectionAgeLimit.HasValue)
            {
                return config.CollectionAgeLimit.Value;
            }
            else
            {
                return DefaultCollectionAgeLimit;
            }
        }

        public void SetCollectionAgeLimit(Type type, TimeSpan ageLimit)
        {
            ThrowIfLessThanZero(ageLimit, "ageLimit");

            GrainTypeConfiguration config;
            if (!_classSpecific.TryGetValue(type.FullName, out config))
            {
                config = new GrainTypeConfiguration(type);
                _classSpecific[type.FullName] = config;
            }

            config.CollectionAgeLimit = ageLimit;
        }

        public void ResetCollectionAgeLimit(Type type)
        {
            GrainTypeConfiguration config;
            if (!_classSpecific.TryGetValue(type.FullName, out config))
            {
                return;
            }

            config.CollectionAgeLimit = null;
        }

        public void SetDefaultCollectionAgeLimit(TimeSpan ageLimit)
        {
            ThrowIfLessThanZero(ageLimit, "ageLimit");
            _defaults.CollectionAgeLimit = ageLimit;
        }

        private void ThrowIfLessThanZero(TimeSpan timeSpan, string paramName)
        {
            if (timeSpan < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(paramName);
            }
        }

    }

    [Serializable]
    public class GrainTypeConfiguration
    {
        public Type Type { get; private set; }
        public bool AreDefaults { get { return Type == null; } }
        public TimeSpan? CollectionAgeLimit { get; set; }

        public GrainTypeConfiguration(Type type)
        {
            Type = type;
        }

        public static GrainTypeConfiguration Load(XmlElement xmlElement, Logger logger)
        {
            Type type = null;
            bool areDefaults = xmlElement.LocalName == "Defaults";
            foreach (XmlAttribute attribute in xmlElement.Attributes)
            {
                if (!areDefaults && attribute.LocalName == "Type")
                {
                    string fullName = attribute.Value.Trim();
                    if (!TypeUtils.TryFindType(fullName, out type))
                    {
                        logger.Error(ErrorCode.Loader_TypeLoadError, string.Format("Unable to find grain class type specified in configuration ({0}); Ignoring.", fullName));
                        return null;
                    }
                }
                else
                {
                    throw new InvalidOperationException(string.Format("unrecognized attribute {0}", attribute.LocalName));
                }
            }
            if (!areDefaults)
            {
                if (type == null)
                {
                    throw new InvalidOperationException("Type attribute not specified");
                }
                // postcondition: returned type must implement IGrain.
                if (!typeof(IGrain).IsAssignableFrom(type))
                {
                    throw new InvalidOperationException(string.Format("Type {0} must implement IGrain to be used in this context", type.FullName));
                }
                // postcondition: returned type must either be an interface or a class.
                if (!type.IsInterface && !type.IsClass)
                {
                    throw new InvalidOperationException(string.Format("Type {0} must either be an interface or class.", type.FullName));
                }
            }
            bool found = false;
            TimeSpan? collectionAgeLimit = null;
            foreach (XmlNode node in xmlElement.ChildNodes)
            {
                XmlElement child = (XmlElement)node;
                switch (child.LocalName)
                {
                    default:
                        throw new InvalidOperationException(string.Format("unrecognized XML element {0}", child.LocalName));
                    case "Deactivation":
                        found = true;
                        collectionAgeLimit = ConfigUtilities.ParseCollectionAgeLimit(child);
                        break;
                }
            }

            if (found)
            {
                return new GrainTypeConfiguration(type)
                       {
                           CollectionAgeLimit = collectionAgeLimit,
                       };
            }
            else
            {
                throw new InvalidOperationException(string.Format("empty GrainTypeConfiguration for {0}", type == null ? "defaults" : type.FullName));
            }
        }
    }
}
