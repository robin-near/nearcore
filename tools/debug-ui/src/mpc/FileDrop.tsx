import React, { ChangeEvent, useState } from 'react';

interface SimpleFileDropProps {
    onFileDrop: (data: string) => void;
}

// Renders a box to drop a file into, and an alternative button to upload.
// onFileDrop is called when a file is dropped or uploaded, with the
// contents of the file as a string.
export const SimpleFileDrop = ({ onFileDrop }: SimpleFileDropProps) => {
    const [drag, setDrag] = useState(false);

    const handleDrag = (e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
    };

    const handleDragIn = (e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setDrag(true);
    };

    const handleDragOut = (e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setDrag(false);
    };

    const handleDrop = (e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setDrag(false);
        if (e.dataTransfer?.files.length > 0) {
            handleFile(e.dataTransfer.files[0]);
        }
    };

    const handleFile = (file: File) => {
        const reader = new FileReader();
        reader.onload = (e) => {
            onFileDrop(e.target!.result as string);
        };
        reader.readAsText(file);
    };

    const handleClick = (event: ChangeEvent<HTMLInputElement>) => {
        const input: HTMLInputElement | null = event.currentTarget;
        if (input && input.files) {
            handleFile(input.files[0]);
        }
    };
    return (
        <div>
            <div
                onDrop={handleDrop}
                onDragEnter={handleDragIn}
                onDragLeave={handleDragOut}
                onDragOver={handleDrag}
                style={{
                    padding: '1rem',
                    border: drag ? '1px solid blue' : '1px solid gray',
                }}>
                Drag and drop a file here
            </div>
            <input type="file" onChange={handleClick} />
        </div>
    );
};
