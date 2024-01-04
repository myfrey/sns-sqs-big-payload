import { S3Client } from '@aws-sdk/client-s3';
import { PublishCommand, PublishCommandOutput, SNSClient } from '@aws-sdk/client-sns';
import { v4 as uuid } from 'uuid';
import { S3PayloadMeta } from './types';
import {
    buildS3PayloadWithExtendedCompatibility,
    buildS3Payload,
    createExtendedCompatibilityAttributeMap,
} from './util';
import { Upload } from '@aws-sdk/lib-storage';

export interface SnsProducerOptions {
    topicArn?: string;
    region?: string;
    largePayloadThoughS3?: boolean;
    allPayloadThoughS3?: boolean;
    s3Bucket?: string;
    sns?: SNSClient;
    s3?: S3Client;
    snsEndpointUrl?: string;
    s3EndpointUrl?: string;
    messageSizeThreshold?: number;
    // Opt-in to enable compatibility with
    // Amazon SQS Extended Client Java Library (and other compatible libraries)
    extendedLibraryCompatibility?: boolean;
}

export interface PublishResult {
    snsResponse: any;
    s3Response?: any;
}

// https://aws.amazon.com/sns/pricing/
// Amazon SNS currently allows a maximum size of 256 KB for published messages.
export const DEFAULT_MAX_SNS_MESSAGE_SIZE = 256 * 1024;

export class SnsProducer {
    private topicArn: string;
    private sns: SNSClient;
    private s3: S3Client;
    private largePayloadThoughS3: boolean;
    private allPayloadThoughS3: boolean;
    private s3Bucket: string;
    private messageSizeThreshold: number;
    private extendedLibraryCompatibility: boolean;

    constructor(options: SnsProducerOptions) {
        if (options.sns) {
            this.sns = options.sns;
        } else {
            this.sns = new SNSClient({
                region: options.region,
                endpoint: options.snsEndpointUrl,
            });
        }
        if (options.allPayloadThoughS3 || options.largePayloadThoughS3) {
            if (!options.s3Bucket) {
                throw new Error(
                    'Need to specify "s3Bucket" option when using allPayloadThoughS3 or  largePayloadThoughS3.'
                );
            }

            if (options.s3) {
                this.s3 = options.s3;
            } else {
                this.s3 = new S3Client({
                    region: options.region,
                    endpoint: options.s3EndpointUrl,
                });
            }
        }

        this.topicArn = options.topicArn;
        this.largePayloadThoughS3 = options.largePayloadThoughS3;
        this.allPayloadThoughS3 = options.allPayloadThoughS3;
        this.s3Bucket = options.s3Bucket;
        this.messageSizeThreshold = options.messageSizeThreshold ?? DEFAULT_MAX_SNS_MESSAGE_SIZE;
        this.extendedLibraryCompatibility = options.extendedLibraryCompatibility;
    }

    public static create(options: SnsProducerOptions): SnsProducer {
        return new SnsProducer(options);
    }

    async publishJSON(message: unknown): Promise<PublishResult> {
        const messageBody = JSON.stringify(message);
        const msgSize = Buffer.byteLength(messageBody, 'utf-8');

        if ((msgSize > this.messageSizeThreshold && this.largePayloadThoughS3) || this.allPayloadThoughS3) {
            const payloadId = uuid();
            const payloadKey = this.extendedLibraryCompatibility ? payloadId : `${payloadId}.json`;
            const uploadToS3 = new Upload({
                client: this.s3,
                params: {
                    Key: payloadKey,
                    Body: messageBody,
                    Bucket: this.s3Bucket,
                    ContentType: 'application/json',
                }, 
            })

            const s3Response = await uploadToS3.done();

            const snsResponse = await this.publishS3Payload(
                {
                    Id: payloadId,
                    Bucket: s3Response.Bucket,
                    Key: s3Response.Key,
                    Location: s3Response.Location,
                },
                msgSize
            );

            return {
                s3Response,
                snsResponse,
            };
        } else if (msgSize > this.messageSizeThreshold) {
            throw new Error(
                `Message is too big (${msgSize} > ${this.messageSizeThreshold}). Use 'largePayloadThoughS3' option to send large payloads though S3.`
            );
        }

        const command = new PublishCommand({
            Message: messageBody,
            TopicArn: this.topicArn,
        });
        const snsResponse = await this.sns.send(command);

        return {
            snsResponse,
        };
    }

    async publishS3Payload(
        s3PayloadMeta: S3PayloadMeta,
        msgSize?: number
    ): Promise<PublishCommandOutput> {
        const messageAttributes = this.extendedLibraryCompatibility
            ? createExtendedCompatibilityAttributeMap(msgSize)
            : {};

        const command = new PublishCommand({
                    Message: this.extendedLibraryCompatibility
                        ? buildS3PayloadWithExtendedCompatibility(s3PayloadMeta)
                        : buildS3Payload(s3PayloadMeta),   
                    TopicArn: this.topicArn,
                    MessageAttributes: messageAttributes,
                });
        const snsResponse = await this.sns.send(command);
        return snsResponse;
    }
}
