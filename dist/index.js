import { CloudWatchLogsClient, CreateLogGroupCommand, CreateLogStreamCommand, DescribeLogStreamsCommand, PutLogEventsCommand } from '@aws-sdk/client-cloudwatch-logs';
import pThrottle from 'p-throttle';
import build from 'pino-abstract-transport';
function isInvalidSequenceTokenException(err) {
    if (err instanceof Error) {
        return err.name === 'InvalidSequenceTokenException';
    }
    return false;
}
function isResourceAlreadyExistsException(err) {
    if (err instanceof Error) {
        return err.name === 'ResourceAlreadyExistsException';
    }
    return false;
}
export default async function (options) {
    const { logGroupName, logStreamName, awsRegion, awsAccessKeyId, awsSecretAccessKey } = options;
    const interval = options.interval || 1000;
    let credentials;
    if (awsAccessKeyId && awsSecretAccessKey) {
        credentials = {
            accessKeyId: awsAccessKeyId,
            secretAccessKey: awsSecretAccessKey
        };
    }
    const client = new CloudWatchLogsClient({ region: awsRegion, credentials });
    let sequenceToken;
    const { addLog, getLogs, wipeLogs, addErrorLog, orderLogs } = (function () {
        let lastFlush = Date.now();
        // https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
        const MAX_EVENT_SIZE = (2 ** 10) * 256; // 256 Kb
        // https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
        const MAX_BUFFER_LENGTH = 10000;
        const MAX_BUFFER_SIZE = 1048576;
        const bufferedLogs = [];
        function reachedNumberOfLogsLimit() {
            return bufferedLogs.length === MAX_BUFFER_LENGTH;
        }
        function reachedBufferSizeLimit(newLog) {
            const currentSize = bufferedLogs.reduce((acc, curr) => acc + curr.message.length + 26, 0);
            return (currentSize + newLog.message.length + 26) >= MAX_BUFFER_SIZE;
        }
        function logEventExceedsSize(log) {
            return log.message.length >= MAX_EVENT_SIZE;
        }
        function getLogs() {
            return bufferedLogs;
        }
        function orderLogs() {
            getLogs().sort((a, b) => a.timestamp - b.timestamp);
        }
        function shouldDoAPeriodicFlush() {
            const now = Date.now();
            const timeSinceLastFlush = now - lastFlush;
            lastFlush = now;
            return timeSinceLastFlush > interval;
        }
        function addLog(log) {
            if (logEventExceedsSize(log)) {
                return false;
            }
            if (!reachedBufferSizeLimit(log)) {
                bufferedLogs.push(log);
                return reachedNumberOfLogsLimit() || shouldDoAPeriodicFlush();
            }
            else {
                setImmediate(() => {
                    addLog(log);
                });
                return true;
            }
        }
        async function addErrorLog(errorLog) {
            const shouldFlush = addLog({
                timestamp: Date.now(),
                message: JSON.stringify(errorLog)
            });
            if (shouldFlush) {
                await flush();
            }
        }
        function wipeLogs() {
            bufferedLogs.length = 0; // TODO: is there a better/more performant way to wipe the array?
        }
        return { addLog, getLogs, wipeLogs, addErrorLog, orderLogs };
    })();
    // Initialization functions
    async function createLogGroup(logGroupName) {
        try {
            await client.send(new CreateLogGroupCommand({ logGroupName }));
        }
        catch (error) {
            if (isResourceAlreadyExistsException(error)) {
                return;
            }
            else {
                throw error;
            }
        }
    }
    async function createLogStream(logGroupName, logStreamName) {
        try {
            await client.send(new CreateLogStreamCommand({
                logGroupName,
                logStreamName
            }));
        }
        catch (error) {
            if (isResourceAlreadyExistsException(error)) {
                return;
            }
            else {
                throw error;
            }
        }
    }
    async function nextToken(logGroupName, logStreamName) {
        const output = await client.send(new DescribeLogStreamsCommand({
            logGroupName,
            logStreamNamePrefix: logStreamName
        }));
        if (output.logStreams?.length === 0) {
            throw new Error('LogStream not found.');
        }
        sequenceToken = output.logStreams?.[0].uploadSequenceToken;
    }
    // Function for putting event logs
    async function putEventLogs(logGroupName, logStreamName, logEvents) {
        if (logEvents.length === 0)
            return;
        try {
            const output = await client.send(new PutLogEventsCommand({
                logEvents,
                logGroupName,
                logStreamName,
                sequenceToken
            }));
            sequenceToken = output.nextSequenceToken;
        }
        catch (error) {
            if (isInvalidSequenceTokenException(error)) {
                sequenceToken = error.expectedSequenceToken;
            }
            else {
                throw error;
            }
        }
    }
    const throttle = pThrottle({
        interval: 1000,
        limit: 1
    });
    const flush = throttle(async function () {
        try {
            orderLogs();
            await putEventLogs(logGroupName, logStreamName, getLogs());
        }
        catch (e) {
            await addErrorLog({ message: 'pino-cloudwatch-transport flushing error', error: e.message });
        }
        finally {
            wipeLogs();
        }
    });
    // Transport initialization
    try {
        await createLogGroup(logGroupName);
        await createLogStream(logGroupName, logStreamName);
        await nextToken(logGroupName, logStreamName);
    }
    catch (e) {
        await addErrorLog({ message: 'pino-cloudwatch-transport initialization error', error: e.message });
    }
    return build(async function (source) {
        for await (const obj of source) {
            try {
                const shouldFlush = addLog(obj);
                if (shouldFlush) {
                    await flush();
                    source.emit('flushed');
                }
            }
            catch (e) {
                console.error('ERROR', e);
                throw e;
            }
        }
    }, {
        parseLine: (line) => {
            let value;
            try {
                value = JSON.parse(line); // TODO: what should be done on failure to parse?
            }
            catch (e) {
                value = '{}';
            }
            return {
                timestamp: value.time || Date.now(),
                message: line
            };
        },
        close: async () => {
            await flush();
            client.destroy();
        }
    });
}
