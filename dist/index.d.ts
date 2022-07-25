/// <reference types="node" />
export interface PinoCloudwatchTransportOptions {
    logGroupName: string;
    logStreamName: string;
    awsRegion?: string;
    awsAccessKeyId?: string;
    awsSecretAccessKey?: string;
    interval?: number;
}
export default function (options: PinoCloudwatchTransportOptions): Promise<import("stream").Transform & import("pino-abstract-transport").OnUnknown>;
