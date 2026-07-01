import { useState } from "react";
import gallerySource from "@/assets/gallery/gallery.json";
interface image {
  url: string;
  credits: string;
  filename: string;
}
export default function GalleryPage() {
  const [showModal, setShowModal] = useState(false);
  const [modalImage, setModalImage] = useState(0);

  const images = gallerySource as image[];

  function showImage(num: number) {
    setShowModal(true);
    setModalImage(num);
  }

  function closeModal() {
    setShowModal(false);
  }

  return (
    <div className="justify-center flex text-white align-center">
      <div className="mt-20 xl:w-2/3">
        <div className="justify-center flex flex-col px-3 pb-10">
          <div className="flex flex-col justify-center items-center">
            <h1 className="smtxt text-8xl font-jaro">Gallery</h1>
            <div className="text-xl text-center font-slackey mt-3">
              <span>Some photos from previous workshops!</span>
            </div>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mt-8">
            {images.map((src, i) => (
              <img
                key={i}
                src={src.url}
                onClick={() => showImage(i)}
                alt={`Credit: ${src.credits}, ${src.filename}`}
                className="w-full h-auto rounded-lg shadow-md hover:cursor-pointer hover:scale-102 transition-all"
              />
            ))}
            {images.length == 0 && (
              <div className="bg-red-600 p-10">
                <span>
                  NO GALLERY IMAGES FOUND AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
                </span>
              </div>
            )}
          </div>
          {showModal && (
            <div
              className="fixed top-0 left-0 w-full h-full bg-black/70 flex items-center justify-center z-50"
              onClick={closeModal}
            >
              <div
                className="relative max-h-[80vh] max-w-[90vw] rounded-lg overflow-hidden"
                onClick={(e) => e.stopPropagation()}
              >
                <img
                  src={images[modalImage].url}
                  alt={`Credit: ${images[modalImage].credits}, ${images[modalImage].filename}`}
                  className="max-w-[90vw] max-h-[75vh] object-contain"
                />
                <button
                  onClick={closeModal}
                  className="absolute top-4 right-4 bg-black/50 hover:bg-black/70 text-white rounded-full w-8 h-8 flex items-center justify-center"
                  aria-label="Close modal"
                >
                  ✕
                </button>
                <span>Credits: {images[modalImage].credits}</span>
                {modalImage > 0 && (
                  <button
                    onClick={() => setModalImage(modalImage - 1)}
                    className="absolute left-4 top-1/2 -translate-y-1/2 bg-black/50 hover:bg-black/70 text-white rounded-full w-10 h-10 flex items-center justify-center"
                    aria-label="Previous image"
                  >
                    ❮
                  </button>
                )}
                {modalImage < images.length - 1 && (
                  <button
                    onClick={() => setModalImage(modalImage + 1)}
                    className="absolute right-4 top-1/2 -translate-y-1/2 bg-black/50 hover:bg-black/70 text-white rounded-full w-10 h-10 flex items-center justify-center"
                    aria-label="Next image"
                  >
                    ❯
                  </button>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
