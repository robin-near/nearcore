import { MouseEvent, useCallback } from 'react';
import './MerkleRangeTreeDisplay.scss';

export type MerkleRangeTreeDisplayProps = {
    minDepthsToDisplay: number;
    treeSize: number;
    showOutsideTree: boolean;
    showRootDecomposition: boolean;

    proofTarget: number | null;
    setProofTarget: (target: number | null) => void;
    partialTreeTarget: number | null;
    setPartialTreeTarget: (target: number | null) => void;
};

type Cell = {
    level: number;
    index: number;
    isInTree: boolean;
    isInRootDecomposition?: boolean;
    isInProofLeft?: boolean;
    isInProofRight?: boolean;
    isProofTarget?: boolean;
    isPartialTreeTarget?: boolean;
    isInPartialTree?: boolean;
}

type CellProps = {
    cell: Cell;
    proofTarget: number | null;
    setProofTarget: (target: number | null) => void;
    partialTreeTarget: number | null;
    setPartialTreeTarget: (target: number | null) => void;
}


const CellElement = ({ cell, proofTarget, setProofTarget, partialTreeTarget, setPartialTreeTarget }: CellProps) => {
    const fills = [];

    if (cell.isInProofLeft) {
        fills.push('lightgreen');
    }
    if (cell.isInProofRight) {
        fills.push('#c2d375');
    }
    if (cell.isProofTarget) {
        fills.push('green');
    }
    if (cell.isPartialTreeTarget) {
        fills.push('blue');
    }
    if (cell.isInPartialTree) {
        fills.push('lightblue');
    }
    if (cell.isInRootDecomposition) {
        fills.push('pink');
    }

    let gradient = '';
    if (fills.length > 1) {
        gradient = "linear-gradient(0deg, ";
        let position = 0;
        for (let i = 0; i < fills.length; i++) {
            if (i > 0) {
                gradient += ",";
            }
            gradient += `${fills[i]} ${position * 100}%`;
            position += 1 / fills.length;
            gradient += `,${fills[i]} ${position * 100}%`;
        }
        gradient += ")";
    } else if (fills.length == 1) {
        gradient = fills[0];
    }

    const style = {
        "--colspan": Math.pow(2, cell.level),
        "background": gradient,
    } as React.CSSProperties;

    const classNames = [
        "cell",
        cell.isInTree ? "" : "out-of-tree",
        `level-${cell.level}`,
    ];
    const clickCallback = useCallback((e: MouseEvent) => {
        e.stopPropagation();
        e.preventDefault();
        if (cell.level == 0) {
            if (cell.index == proofTarget) { setProofTarget(null); }
            else if (cell.isInTree) { setProofTarget(cell.index); }
        }

    }, [cell, proofTarget, setProofTarget]);
    const rightClickCallback = useCallback((e: MouseEvent) => {
        e.stopPropagation();
        e.preventDefault();
        if (cell.level == 0) {
            if (cell.index == partialTreeTarget) { setPartialTreeTarget(null); }
            else { setPartialTreeTarget(cell.index); }
        }

    }, [cell, partialTreeTarget, setPartialTreeTarget]);

    return <div className="cell-container"
        onClick={clickCallback}
        onContextMenu={rightClickCallback}>
        <div style={style} className={classNames.join(" ")}></div>
    </div>
};

export const MerkleRangeTreeDisplay = (props: MerkleRangeTreeDisplayProps) => {
    const depth = Math.ceil(Math.log2(props.treeSize));
    const tree: Cell[][] = [];

    for (let level = 0; level <= depth; level++) {
        const levelCells: Cell[] = [];
        for (let j = 0; j < (1 << depth >> level); j++) {
            const isInTree = (j + 1) * (1 << level) <= props.treeSize;
            levelCells.push({
                level,
                index: j,
                isInTree,
            })
        }
        tree.push(levelCells);
    }

    if (props.showRootDecomposition) {
        let n = props.treeSize;
        let level = 0;
        while (n > 0) {
            if (n % 2 == 1) {
                const j = n - 1;
                tree[level][j].isInRootDecomposition = true;
            }
            level += 1;
            n = Math.floor(n / 2);
        }
    }

    if (props.partialTreeTarget !== null && props.partialTreeTarget < (1 << depth)) {
        tree[0][props.partialTreeTarget].isPartialTreeTarget = true;

        let n = props.partialTreeTarget;
        let level = 0;
        while (n > 0) {
            if (n % 2 == 1) {
                const j = n - 1;
                tree[level][j].isInPartialTree = true;
            }
            level += 1;
            n = Math.floor(n / 2);
        }
    }

    if (props.proofTarget !== null && props.proofTarget < props.treeSize) {
        tree[0][props.proofTarget].isProofTarget = true;

        let n = props.proofTarget;
        let level = 0;
        while (n > 0) {
            if (n % 2 == 1) {
                const j = n - 1;
                tree[level][j].isInProofLeft = true;
            }
            level += 1;
            n = Math.floor(n / 2);
        }

        n = props.proofTarget + 1;
        level = 0;
        while ((n + 1) * (1 << level) <= props.treeSize) {
            if (n % 2 == 1) {
                const j = n;
                tree[level][j].isInProofRight = true;
            }
            level += 1;
            n = Math.ceil(n / 2);
        }
        let levelToUseForRightTail = level;

        {
            let n = props.treeSize;
            let level = 0;
            while (n > 0) {
                if (n % 2 == 1 && level < levelToUseForRightTail) {
                    const j = n - 1;
                    tree[level][j].isInProofRight = true;
                }
                level += 1;
                n = Math.floor(n / 2);
            }
        }
    }

    const treeEles = [];
    for (let i = depth + 1; i < props.minDepthsToDisplay; i++) {
        treeEles.push(<div key={i} className="level"></div>);
    }
    for (let level = depth; level >= 0; level--) {
        const levelEles = [];
        for (let j = 0; j < tree[level].length; j++) {
            const cell = tree[level][j];
            if (!props.showOutsideTree && !cell.isInTree) {
                continue;
            }
            levelEles.push(<CellElement key={j}
                cell={cell}
                proofTarget={props.proofTarget} setProofTarget={props.setProofTarget}
                partialTreeTarget={props.partialTreeTarget} setPartialTreeTarget={props.setPartialTreeTarget}
            />);
        }
        treeEles.push(<div key={level} className="level">{levelEles}</div>);
    }

    const style = {
        "--width-scale": Math.floor(1920 / (1 << depth)) + "px",
    } as React.CSSProperties;

    return (
        <div style={style} className="merkle-tree" onContextMenu={(e) => e.preventDefault()}>
            {treeEles}
        </div>
    );

};
