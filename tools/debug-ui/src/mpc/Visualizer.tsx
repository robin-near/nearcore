import { useCallback, useState } from "react";
import { SimpleFileDrop } from "./FileDrop";
import './Visualizer.scss';

interface NetworkResearchReport {
    num_participants: number;
    steps: NetworkStep[];
}

interface NetworkStep {
    peer_to_peer: PeerToPeerMessageStats[][];
}

interface PeerToPeerMessageStats {
    num_messages: number;
    total_bytes: number;
}

interface MpcNetworkVisualizerProps {
    report: NetworkResearchReport;
}

export const MpcNetworkVisualizer: React.FC<MpcNetworkVisualizerProps> = ({ report }) => {
    let participantIds = [...Array(report.num_participants).keys()];

    return (
        <div className="mpc-network-visualizer" style={getGlobalStyles()}>
            <div className="participant-headings">
                {participantIds.map((participantId) => (
                    <div key={participantId} className="participant-heading">
                        Participant {participantId}
                    </div>
                ))}
            </div>
            <div className="main-diagram">
                {report.steps.map((step, stepIndex) => (
                    <div key={stepIndex} className="step">
                        {step.peer_to_peer.map((peerToPeerMessages, participantId) => (
                            <div key={participantId} className="participant-step">
                                <div className="incoming-dot"></div>
                                {peerToPeerMessages.map((peerToPeerMessageStats, peerId) => (
                                    <div key={peerId} className="p2p-stats">

                                        <div className="num-messages">
                                            {peerToPeerMessageStats.num_messages}
                                        </div>
                                        <div className="total-bytes">
                                            <span className={bytesClass(peerToPeerMessageStats.total_bytes)}>
                                                {formatBytes(peerToPeerMessageStats.total_bytes)}
                                            </span>
                                        </div>
                                        <div className="outgoing-dot"></div>
                                    </div>
                                ))}
                            </div>
                        ))}
                    </div>
                ))}
            </div>
            <div className="arrows">
                {report.steps.flatMap((step, stepIndex) =>
                    step.peer_to_peer.flatMap((peerToPeerMessages, from) =>
                        peerToPeerMessages.flatMap((peerToPeerMessageStats, to) =>
                            peerToPeerMessageStats.num_messages > 0 ? [getElementForArrow(report.num_participants, stepIndex, from, to, peerToPeerMessageStats.total_bytes)] : []
                        )))}
            </div>
        </div>
    );
}

function formatBytes(bytes: number): string {
    if (bytes == 0) {
        return '0';
    }
    if (bytes < 1000) {
        return `${bytes} B`;
    }
    if (bytes < 1024 * 1024) {
        return `${(bytes / 1024).toFixed(0)} K`;
    }
    return `${(bytes / 1024 / 1024).toFixed(0)} M`;
}

function bytesClass(bytes: number): string {
    if (bytes == 0) {
        return ''
    }
    if (bytes < 1024) {
        return 'little';
    }
    if (bytes < 1024 * 20) {
        return 'some';
    }
    return 'lots';
}

const COLUMN_WIDTH = 320;
const TOP_MARGIN = 30;
const LEFT_MARGIN = 10;
const HEADING_HEIGHT = 50;
const STEP_HEIGHT = 500;
const STATS_WIDTH = 30;
const STATS_HEIGHT = 60;

function getGlobalStyles(): React.CSSProperties {
    return {
        "--column-width": COLUMN_WIDTH + "px",
        "--top-margin": TOP_MARGIN + "px",
        "--left-margin": LEFT_MARGIN + "px",
        "--heading-height": HEADING_HEIGHT + "px",
        "--step-height": STEP_HEIGHT + "px",
        "--stats-width": STATS_WIDTH + "px",
        "--stats-height": STATS_HEIGHT + "px",
    } as React.CSSProperties;
}

function getElementForArrow(n: number, step: number, from: number, to: number, numBytes: number): JSX.Element {
    const sourceDotX = from * COLUMN_WIDTH + LEFT_MARGIN + COLUMN_WIDTH / 2 - STATS_WIDTH * n / 2 + STATS_WIDTH / 2 + (STATS_WIDTH * to);
    const sourceDotY = TOP_MARGIN + HEADING_HEIGHT + step * STEP_HEIGHT + STEP_HEIGHT / 2 + STATS_HEIGHT / 2;
    const targetDotX = to * COLUMN_WIDTH + LEFT_MARGIN + COLUMN_WIDTH / 2;
    const targetDotY = TOP_MARGIN + HEADING_HEIGHT + (step + 1) * STEP_HEIGHT + STEP_HEIGHT / 2 - STATS_HEIGHT / 2;

    const minX = Math.min(sourceDotX, targetDotX) - 20;
    const minY = Math.min(sourceDotY, targetDotY) - 20;
    const maxX = Math.max(sourceDotX, targetDotX) + 20;
    const maxY = Math.max(sourceDotY, targetDotY) + 20;

    let arrowWidth;
    if (numBytes < 1024) {
        arrowWidth = 1;
    } else {
        arrowWidth = 1 + Math.log2(numBytes / 1024) / 2;
    }
    return <svg className="arrow" width={maxX - minX} height={maxY - minY} style={{ left: minX, top: minY }}>
        <line className={bytesClass(numBytes)} x1={sourceDotX - minX} y1={sourceDotY - minY} x2={targetDotX - minX} y2={targetDotY - minY}
            style={{ strokeWidth: arrowWidth }} />
    </svg>;
}

export const MpcVisualizer = () => {
    let [data, setData] = useState<NetworkResearchReport | null>(null);

    const callback = useCallback((data: string) => {
        let parsed = JSON.parse(data);
        setData(parsed);
    }, []);

    return (
        <div className="mpc-visualizer">
            {data === null && <SimpleFileDrop onFileDrop={callback} />}
            {data && <MpcNetworkVisualizer report={data} />}
        </div>
    );
}