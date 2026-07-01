import { useEffect } from "react";

interface ImageModalProps {
  isOpen: boolean;
  image: string;
  onClose: () => void;
}

export default function ImageModal({ isOpen, image, onClose }: ImageModalProps) {
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        onClose();
      }
    };

    if (isOpen) {
      document.addEventListener("keydown", handleEscape);
      document.body.style.overflow = "hidden";
    }

    return () => {
      document.removeEventListener("keydown", handleEscape);
      document.body.style.overflow = "unset";
    };
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70"
      onClick={onClose}
    >
      <div
        className="relative max-h-[90vh] max-w-[90vw] overflow-hidden rounded-lg"
        onClick={(e) => e.stopPropagation()}
      >
        <img
          src={image}
          alt="Extended view"
          className="h-full w-full object-contain"
        />
        <button
          onClick={onClose}
          className="absolute right-4 top-4 flex h-8 w-8 items-center justify-center rounded-full bg-black/50 text-white hover:bg-black/70"
          aria-label="Close modal"
        >
          ✕
        </button>
      </div>
    </div>
  );
}
