import { MessageAttributeValue } from '@aws-sdk/client-sns';
import { S3PayloadMeta, PayloadMeta, SqsExtendedPayloadMeta } from './types';
import { AMAZON_EXTENDED_CLIENT_PAYLOAD_OFFLOADING_REFERENCE, SQS_LARGE_PAYLOAD_SIZE_ATTRIBUTE } from './constants';

export function createExtendedCompatibilityAttributeMap(msgSize: number): Record<string, MessageAttributeValue> {
    const result = {};
    result[SQS_LARGE_PAYLOAD_SIZE_ATTRIBUTE] = {
        StringValue: '' + msgSize,
        DataType: 'Number',
    };
    return result;
}

export function buildS3Payload(s3PayloadMeta: S3PayloadMeta): string {
    return JSON.stringify({
        S3Payload: s3PayloadMeta,
    } as PayloadMeta);
}

export function buildS3PayloadWithExtendedCompatibility(s3PayloadMeta: S3PayloadMeta): string {
    return JSON.stringify(
        [AMAZON_EXTENDED_CLIENT_PAYLOAD_OFFLOADING_REFERENCE, {
            s3BucketName: s3PayloadMeta.Bucket,
            s3Key: s3PayloadMeta.Key,
        }] as SqsExtendedPayloadMeta);
}
