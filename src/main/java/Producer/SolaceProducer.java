package Producer;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

public class SolaceProducer extends RichSourceFunction<String> {
	String serverUrl = "ssl://mrjfgwkeg1cer.messaging.solace.cloud:8883";
	String topic = "create/iot-control/CA/ON/device/302220601105034/status";
	String cilent_id = "mqtt.client.id";
	String caFilePath = "C:\\Users\\salta\\Desktop\\work\\ssl\\DigiCertGlobalCA-1.crt.pem";
	String clientCrtFilePath = "C:\\Users\\salta\\Desktop\\work\\ssl\\302220601105034.pem.crt";
	String clientKeyFilePath = "C:\\Users\\salta\\Desktop\\work\\ssl\\302220601105034.key";
	MqttClient mqttClient;
	boolean running = false;
	Object waitLock;

	private static SSLSocketFactory getSocketFactory(final String caCrtFile,
	final String crtFile, final String keyFile, final String password)
	throws Exception {
	Security.addProvider(new BouncyCastleProvider());

	// load CA certificate
	X509Certificate caCert = null;

	FileInputStream fis = new FileInputStream(caCrtFile);
	System.out.println("===========" + caCrtFile);
	BufferedInputStream bis = new BufferedInputStream(fis);
	CertificateFactory cf = CertificateFactory.getInstance("X.509");

	while (bis.available() > 0) {
	caCert = (X509Certificate) cf.generateCertificate(bis);
	// System.out.println(caCert.toString());
	}

	// load client certificate
	bis = new BufferedInputStream(new FileInputStream(crtFile));
	System.out.println("===========" + crtFile);
	X509Certificate cert = null;
	while (bis.available() > 0) {
	cert = (X509Certificate) cf.generateCertificate(bis);
	// System.out.println(caCert.toString());
	}

	// load client private key
	PEMParser pemParser = new PEMParser(new FileReader(keyFile));
	System.out.println("===========" + keyFile);
	Object object = pemParser.readObject();
	PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder()
	.build(password.toCharArray());
	JcaPEMKeyConverter converter = new JcaPEMKeyConverter()
	.setProvider("BC");
	KeyPair key;
	if (object instanceof PEMEncryptedKeyPair) {
	System.out.println("Encrypted key - we will use provided password");
	key = converter.getKeyPair(((PEMEncryptedKeyPair) object)
	.decryptKeyPair(decProv));
	} else {
	System.out.println("Unencrypted key - no password needed");
	key = converter.getKeyPair((PEMKeyPair) object);
	}
	pemParser.close();

	// CA certificate is used to authenticate server
	KeyStore caKs = KeyStore.getInstance(KeyStore.getDefaultType());
	caKs.load(null, null);
	caKs.setCertificateEntry("ca-certificate", caCert);
	TrustManagerFactory tmf = TrustManagerFactory.getInstance("X509");
	tmf.init(caKs);

	// client key and certificates are sent to server so it can authenticate
	// us
	KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
	ks.load(null, null);
	ks.setCertificateEntry("certificate", cert);
	ks.setKeyEntry("private-key", key.getPrivate(), password.toCharArray(),
	new java.security.cert.Certificate[] { cert });
	KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
	.getDefaultAlgorithm());
	kmf.init(ks, password.toCharArray());

	// finally, create SSL socket factory
	SSLContext context = SSLContext.getInstance("TLSv1.2");
	context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

	return context.getSocketFactory();
	}



    @Override
    public void open(Configuration parameters) throws Exception {
        mqttClient = new MqttClient(serverUrl, "HelloWorldSub");
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        SSLSocketFactory socketFactory = getSocketFactory(caFilePath,
					clientCrtFilePath, clientKeyFilePath, "");
        connOpts.setSocketFactory(socketFactory);
        System.out.println("starting connect the server...");
        mqttClient.connect(connOpts);
        System.out.println("connected!");
		// Thread.sleep(1000);
    }

    @Override
    public void cancel() {
		// TODO Auto-generated method stub
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
		// TODO Auto-generated method stub
		mqttClient.subscribe(topic, (topic, message) -> {
            String msg = new String(message.getPayload(), StandardCharsets.UTF_8);
            ctx.collect(msg);
        });

        running = true;
        waitLock = new Object();

        while (running) {
            synchronized (waitLock) {
                waitLock.wait(100L);
            }
        }
    }
    @Override
    public void close() throws Exception {
        super.close();
		try {
            if (mqttClient != null) {
				mqttClient.disconnect();
				System.out.println("disconnected!");
            }
        } catch (MqttException exception) {

        } finally {
            this.running = false;
        }
		// leave main method
		if (waitLock != null) {
			synchronized (waitLock) {
				waitLock.notify();
			}
		}
    }

}