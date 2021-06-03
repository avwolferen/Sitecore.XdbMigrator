using Sitecore.Framework.Conditions;
using Sitecore.Xdb.Common.Web;
using System;
using System.IO;
using System.Net.Http;
using System.Security;
using System.Security.Cryptography.X509Certificates;

namespace AlexVanWolferen.SitecoreXdbMigrator
{
    public class PfxCertificateHttpClientHandlerModifier : IHttpClientHandlerModifier
    {
		public string PfxFileLocation
		{
			get;
			protected set;
		}

		public SecureString PfxPassword
		{
			get;
			protected set;
		}

        public PfxCertificateHttpClientHandlerModifier(string filePath, SecureString password) : this()
        {
			PfxFileLocation = filePath;
			PfxPassword = password;
        }

		public PfxCertificateHttpClientHandlerModifier(string filePath) : this(filePath, null)
		{
		}

		public PfxCertificateHttpClientHandlerModifier()
        {
        }

		public void Process(HttpClientHandler handler)
        {
			Condition.Requires(handler, "handler").IsNotNull();
			if (!string.IsNullOrEmpty(PfxFileLocation) && File.Exists(PfxFileLocation))
			{
				X509Certificate x509Certificate =
					PfxPassword == null || PfxPassword.Length == 0
						? new X509Certificate(PfxFileLocation)
						: new X509Certificate(PfxFileLocation, PfxPassword);

				if (x509Certificate == null)
				{
					throw new InvalidOperationException($"Pfx not found at {PfxFileLocation}");
				}
				handler.ClientCertificateOptions = ClientCertificateOption.Manual;
				handler.ClientCertificates.Add(x509Certificate);
			}
		}
    }
}
