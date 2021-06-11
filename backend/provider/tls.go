package provider

import (
	"backend/config"
	"crypto/tls"
	"crypto/x509"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
)

type TLSConfig struct {
	CertificateAuthority string
	Certificate          string
	PrivateKey           string
}

func NewConfig(config config.TLS) *TLSConfig {
	return &TLSConfig{
		CertificateAuthority: config.CertificateAuthority,
		Certificate:          config.Certificate,
		PrivateKey:           config.PrivateKey,
	}
}

func (self *TLSConfig) TLSConfig() (*tls.Config, error) {
	var cfg = &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Authority
	if self.CertificateAuthority != "" {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert

		var caCrt, err = ioutil.ReadFile(self.CertificateAuthority)
		if err != nil {
			log.Error("Read CertificateAuthority error: %s", err.Error())
			return nil, err
		}

		cfg.RootCAs = x509.NewCertPool()
		if ok := cfg.RootCAs.AppendCertsFromPEM(caCrt); !ok {
			log.Error("failed to apply root CA '%s' to root certificate pool", self.CertificateAuthority)
			return nil, err
		}
	}

	// Certificates
	if self.Certificate != "" && self.PrivateKey != "" {
		var crt, err = tls.LoadX509KeyPair(self.Certificate, self.PrivateKey)
		if err != nil {
			log.Error("Load keypair error: %s", err.Error())
			return nil, err
		}

		cfg.Certificates = []tls.Certificate{crt}
	}

	return cfg, nil
}
