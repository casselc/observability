// credstubs stands in for the AWS side of each credential mode, so that
// clickhouse local, a ClickHouse server or chDB can be shown to resolve
// credentials without keys in the query (parquetgo README, "Credentials and
// deployment targets"). Every stub hands out the store's key and secret
// (-key, -secret) and the session token in $STUB_TOKEN (default empty:
// SeaweedFS rejects tokens its own STS did not issue), expiring after
// $STUB_TTL (default 1h). It writes ca.pem (a fresh private CA) and logs
// every request as a JSON line to stub.log, both in the working directory.
//
//	:18901  http  container-credentials (GET) and STS (POST) stub
//	:18902  http  CONNECT proxy that terminates TLS for ANY host with a leaf
//	              signed by ca.pem and serves the same stub (used as https_proxy
//	              so ClickHouse's hard-coded https://sts.<region>.amazonaws.com
//	              lands here)
//	:18904  http  IMDSv2 emulation, as `aws_signing_helper serve` provides
//	              for IAM Roles Anywhere (token PUT, role list, role creds)
//	:18903  https reverse proxy to SeaweedFS (cert for 127.0.0.1 from ca.pem),
//	              forwarding the client's Host header unchanged
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	logMu  sync.Mutex
)

func logj(v map[string]any) {
	logMu.Lock()
	defer logMu.Unlock()
	f, _ := os.OpenFile("stub.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	json.NewEncoder(f).Encode(v)
	f.Close()
}

func leaf(host string) tls.Certificate {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: host},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	} else {
		tmpl.DNSNames = []string{host}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &k.PublicKey, caKey)
	if err != nil {
		log.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: k}
}

func stub(w http.ResponseWriter, r *http.Request) {
	tok := os.Getenv("STUB_TOKEN")
	ttl, _ := time.ParseDuration(os.Getenv("STUB_TTL"))
	if ttl == 0 {
		ttl = time.Hour
	}
	exp := time.Now().Add(ttl).UTC().Format(time.RFC3339)
	if r.Method == http.MethodGet {
		logj(map[string]any{"m": "GET", "host": r.Host, "path": r.URL.Path, "auth": r.Header.Get("Authorization")})
		json.NewEncoder(w).Encode(map[string]string{"AccessKeyId": *key, "SecretAccessKey": *secret, "Token": tok, "Expiration": exp})
		return
	}
	r.ParseForm()
	form := map[string]string{}
	for k, v := range r.Form {
		s := v[0]
		if len(s) > 60 {
			s = s[:60]
		}
		form[k] = s
	}
	a := r.Form.Get("Action")
	logj(map[string]any{"m": "POST", "host": r.Host, "form": form})
	w.Header().Set("Content-Type", "text/xml")
	fmt.Fprintf(w, `<%[1]sResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><%[1]sResult><Credentials><AccessKeyId>%[4]s</AccessKeyId><SecretAccessKey>%[5]s</SecretAccessKey><SessionToken>%[2]s</SessionToken><Expiration>%[3]s</Expiration></Credentials><AssumedRoleUser><Arn>arn:aws:sts::1:assumed-role/r/s</Arn><AssumedRoleId>A:s</AssumedRoleId></AssumedRoleUser></%[1]sResult><ResponseMetadata><RequestId>stub</RequestId></ResponseMetadata></%[1]sResponse>`, a, tok, exp, *key, *secret)
}

type oneConn struct {
	c    net.Conn
	once sync.Once
	done chan struct{}
}

func (l *oneConn) Accept() (net.Conn, error) {
	var c net.Conn
	l.once.Do(func() { c = l.c })
	if c != nil {
		return c, nil
	}
	<-l.done
	return nil, net.ErrClosed
}
func (l *oneConn) Close() error   { return nil }
func (l *oneConn) Addr() net.Addr { return l.c.LocalAddr() }

func connectProxy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		http.Error(w, "CONNECT only", 405)
		return
	}
	host, _, _ := net.SplitHostPort(r.Host)
	logj(map[string]any{"m": "CONNECT", "host": r.Host})
	c, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	c.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))
	cert := leaf(host)
	tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}})
	done := make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(stub), ConnState: func(_ net.Conn, s http.ConnState) {
		if s == http.StateClosed {
			close(done)
		}
	}}
	go srv.Serve(&oneConn{c: tc, done: done})
}

var key, secret = flag.String("key", "otel", "access key handed out"), flag.String("secret", "otelsecret", "secret handed out")

func main() {
	upstream := flag.String("s3", "http://127.0.0.1:18333", "S3 store behind the TLS proxy on :18903")
	flag.Parse()
	caKey, _ = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "otel-chdb test private CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &caKey.PublicKey, caKey)
	caCert, _ = x509.ParseCertificate(der)
	os.WriteFile("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644)

	go func() { log.Fatal(http.ListenAndServe("127.0.0.1:18901", http.HandlerFunc(stub))) }()
	go func() { log.Fatal(http.ListenAndServe("127.0.0.1:18904", http.HandlerFunc(imds))) }()
	go func() { log.Fatal(http.ListenAndServe("127.0.0.1:18902", http.HandlerFunc(connectProxy))) }()
	up, _ := url.Parse(*upstream)
	rp := httputil.NewSingleHostReverseProxy(up)
	inner := rp.Director
	rp.Director = func(r *http.Request) {
		logj(map[string]any{"m": "S3", "method": r.Method, "host": r.Host, "path": r.URL.Path, "token": r.Header.Get("X-Amz-Security-Token")})
		inner(r)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:18903", &tls.Config{Certificates: []tls.Certificate{leaf("127.0.0.1")}})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(http.Serve(ln, rp))
}

func imds(w http.ResponseWriter, r *http.Request) {
	logj(map[string]any{"m": "IMDS " + r.Method, "path": r.URL.Path, "imds_token": r.Header.Get("X-Aws-Ec2-Metadata-Token")})
	switch {
	case r.Method == http.MethodPut && r.URL.Path == "/latest/api/token":
		w.Header().Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
		fmt.Fprint(w, "imds-session-token")
	case strings.TrimSuffix(r.URL.Path, "/") == "/latest/meta-data/iam/security-credentials":
		fmt.Fprint(w, "otel-publisher")
	case r.URL.Path == "/latest/meta-data/iam/security-credentials/otel-publisher":
		now := time.Now().UTC()
		json.NewEncoder(w).Encode(map[string]string{"Code": "Success", "Type": "AWS-HMAC", "AccessKeyId": *key,
			"SecretAccessKey": *secret, "Token": os.Getenv("STUB_TOKEN"), "LastUpdated": now.Format(time.RFC3339),
			"Expiration": now.Add(time.Hour).Format(time.RFC3339)})
	default:
		http.NotFound(w, r)
	}
}
