package org.apache.hadoop.fs.s3a.sts;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.*;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.sts.Constants4STS.*;

import java.util.ArrayList;
import java.util.List;

public class STSCredentialsProvider {

    private final String roleARN;

    private final String roleSessionName;

    private final String signingRegion;

    private final boolean specifyRoleExternalId;

    private final String roleExternalId;

    private final boolean specifySTSEndpoint;

    private final String stsEndpoint;

    private final boolean specifySessionDuration;

    private final int sessionDuration;

    private final boolean specifySerialNum;

    private final String serialNumber;

    private final boolean specifyTokenCode;

    private final String tokenCode;

    private final boolean specifyTags;

    private final String tags;

    private final boolean specifyPolicyJson;

    private final String policyJson;

    private final boolean specifyPolicyARNs;

    private final String policyARNs;

    public STSCredentialsProvider(Configuration conf) {
        roleARN = conf.get(STS_ROLE_ARN,null);
        roleSessionName = conf.get(STS_ROLE_SESSION_NAME,null);
        signingRegion = conf.get(STS_SIGNING_REGION,"us-east-1");
        specifyRoleExternalId = conf.getBoolean(STS_SPECIFY_ROLE_EXTERNAL_ID,false);
        roleExternalId = conf.get(STS_ROLE_EXTERNAL_ID,null);
        specifySTSEndpoint = conf.getBoolean(STS_SPECIFY_ENDPOINT,false);
        stsEndpoint = conf.get(STS_ENDPOINT,null);
        specifySessionDuration = conf.getBoolean(STS_SPECIFY_SESSION_DURATION,false);
        sessionDuration = conf.getInt(STS_SESSION_DURATION,15);
        specifySerialNum = conf.getBoolean(STS_SPECIFY_SERIAL_NUM,false);
        serialNumber = conf.get(STS_SERIAL_NUMBER,null);
        specifyTokenCode = conf.getBoolean(STS_SPECIFY_TOKEN_CODE,false);
        tokenCode = conf.get(STS_TOKEN_CODE,null);
        specifyTags = conf.getBoolean(STS_SPECIFY_TAGS,false);
        specifyPolicyJson = conf.getBoolean(STS_SPECIFY_POLICY_JSON,false);
        policyJson = conf.get(STS_POLICY_JSON,null);
        specifyPolicyARNs = conf.getBoolean(STS_SPECIFY_POLICY_ARNS,false);
        tags = conf.get(STS_TAGS, null);
        policyARNs = conf.get(STS_POLICY_ARNS, null);
    }

    public AWSCredentials getSTSCredentials(AWSCredentials basicCredentials) {
        AWSSecurityTokenServiceClientBuilder stsClientBuilder = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(basicCredentials));
        if (specifySTSEndpoint) {
            stsClientBuilder
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(stsEndpoint, signingRegion));
        } else {
            stsClientBuilder.withRegion(signingRegion);
        }

        AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest().withRoleArn(roleARN).withRoleSessionName(roleSessionName);

        if (specifyRoleExternalId) {
            assumeRoleRequest.withExternalId(roleExternalId);
        }

        if (specifySessionDuration) {
            assumeRoleRequest.withDurationSeconds(sessionDuration * 60);
        }

        if (specifySerialNum) {
            assumeRoleRequest.withSerialNumber(serialNumber);
        }

        if (specifyTokenCode) {
            assumeRoleRequest.withTokenCode(tokenCode);
        }

        if (specifyTags) {
            List<Tag> tagList = new ArrayList<Tag>();
            List<String> tranTagKeys = new ArrayList<String>();

            String[] tagArray = tags.split(";");

            for (String tagInfo : tagArray) {
                String[] t = tagInfo.split(",");
                Tag tag = new Tag().withKey(t[0]).withValue(t[1]);

                tagList.add(tag);

                if ("true".equalsIgnoreCase(t[2]))
                    tranTagKeys.add(t[0]);
            }

            assumeRoleRequest.withTags(tagList);
            assumeRoleRequest.withTransitiveTagKeys(tranTagKeys);
        }

        if (specifyPolicyJson) {
            assumeRoleRequest.withPolicy(policyJson);
        }

        if (specifyPolicyARNs) {
            List<PolicyDescriptorType> policyARNList = new ArrayList<PolicyDescriptorType>();
            String[] policyArray = policyARNs.split(",");

            for (String arn : policyArray) {
                policyARNList.add(new PolicyDescriptorType().withArn(arn));
            }
            assumeRoleRequest.withPolicyArns(policyARNList);
        }

        AssumeRoleResult assumeRoleResult = stsClientBuilder.build().assumeRole(assumeRoleRequest);
        Credentials assumeRoleCred = assumeRoleResult.getCredentials();
        BasicSessionCredentials roleSessionCred = new BasicSessionCredentials(assumeRoleCred.getAccessKeyId(),
                assumeRoleCred.getSecretAccessKey(), assumeRoleCred.getSessionToken());

        return roleSessionCred;
    }
}
