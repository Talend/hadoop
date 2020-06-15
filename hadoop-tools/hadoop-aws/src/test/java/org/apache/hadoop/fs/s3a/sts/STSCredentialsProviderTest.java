package org.apache.hadoop.fs.s3a.sts;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.hadoop.fs.s3a.sts.Constants4STS.*;

@Ignore("ignore now as no accout in test env")
public class STSCredentialsProviderTest {

    @Test
    public void testSTS() {
        Configuration conf = new Configuration();
        conf.setBoolean(STS_SPECIFY, true);
        conf.set(STS_ROLE_ARN, System.getProperty("s3.roleARN"));
        conf.set(STS_ROLE_SESSION_NAME, "test_STS");
        conf.setInt(STS_SESSION_DURATION, 20);
        conf.set(STS_SIGNING_REGION, "us-east-1");
        conf.setBoolean(STS_SPECIFY_ENDPOINT, true);
        conf.set(STS_ENDPOINT, "sts.us-east-1.amazonaws.com");
        conf.setBoolean(STS_SPECIFY_TAGS, true);
        conf.set(STS_TAGS, "key1,val1,true;key2,val2,false");
        conf.setBoolean(STS_SPECIFY_POLICY_ARNS, true);
        conf.set(STS_POLICY_ARNS, "arn1,arn2,arn3");

        STSCredentialsProvider stsCred = new STSCredentialsProvider(conf);

        AWSCredentials credentials =
                new BasicAWSCredentials(System.getProperty("s3.accesskey"), System.getProperty("s3.secretkey"));

        stsCred.getSTSCredentials(credentials);
    }
}