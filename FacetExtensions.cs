using Sitecore.XConnect;
using Sitecore.XConnect.Serialization;

namespace AlexVanWolferen.SitecoreXdbMigrator
{
    public static class FacetExtensions
    {
        public static T WithClearedConcurrency<T>(this T facet) where T : Facet
        {
            DeserializationHelpers.SetConcurrencyToken(facet, null);
            return facet;
        }
    }
}
