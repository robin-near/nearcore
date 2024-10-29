import { useState } from "react";
import { MerkleRangeTreeDisplay } from "./MerkleRangeTreeDisplay";

export const MerkleTreeApp = () => {
    const [treeSize, setTreeSize] = useState(14);
    const [showOutsideTree, setShowOutsideTree] = useState(false);
    const [showRootDecomposition, setShowRootDecomposition] = useState(true);
    const [proofTarget, setProofTarget] = useState<number | null>(null);
    const [partialTreeTarget, setPartialTreeTarget] = useState<number | null>(null);

    return (
        <div className="MerkleTreeApp">
            <h1>Merkle Tree</h1>

            <div className="controls">
                <div className="control">
                    Tree size: <input
                        type="number"
                        value={treeSize}
                        onChange={(e) => setTreeSize(parseInt(e.target.value))} />
                </div>
                <div className="control">
                    <input
                        id="showOutsideTree"
                        type="checkbox"
                        checked={showOutsideTree}
                        onChange={(e) => setShowOutsideTree(e.target.checked)} />
                    <label htmlFor="showOutsideTree">Show Zeros</label>
                </div>
                <div className="control">
                    <input
                        id="showRootDecomposition"
                        type="checkbox"
                        checked={showRootDecomposition}
                        onChange={(e) => setShowRootDecomposition(e.target.checked)} />
                    <label htmlFor="showRootDecomposition">Show Root Decomposition</label>
                </div>
            </div>

            <MerkleRangeTreeDisplay
                treeSize={treeSize}
                minDepthsToDisplay={5}
                showOutsideTree={showOutsideTree}
                showRootDecomposition={showRootDecomposition}

                proofTarget={proofTarget}
                setProofTarget={setProofTarget}

                partialTreeTarget={partialTreeTarget}
                setPartialTreeTarget={setPartialTreeTarget}
            />
        </div>
    );
}