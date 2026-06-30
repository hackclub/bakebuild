import {
  Carousel,
  CarouselContent,
  CarouselItem,
  CarouselNext,
  CarouselPrevious,
} from "@/components/ui/carousel";

export default function HomeGallery() {
  const images = [
    "https://cdn.hackclub.com/019ecc7c-d7dd-75a7-8add-7d85c6d50e23/1000088286.webp",
    "https://cdn.hackclub.com/019ecc7c-d9c9-7011-856f-544ae9fdd4f3/1000088291.webp",
    "https://cdn.hackclub.com/019ecc7d-58fa-7a1b-8ea1-ff66d13695c5/1000019066.webp",
    "https://cdn.hackclub.com/019ecc7b-65bd-7f07-918a-9f35488bd72f/IMG_9047.webp",
    "https://cdn.hackclub.com/019ecc77-a821-7ffc-b0a7-60777617cc58/DSCF5723.webp",
  ];
  return (
    <Carousel
      opts={{
        align: "center",
        loop: true,
      }}
      className="mx-auto w-full max-w-6xl"
    >
      <CarouselContent className="-ml-2">
        {Array.from(images).map((image, index) => (
          <CarouselItem key={index} className="pl-2 sm:basis-1/2 lg:basis-1/3">
            <div className="overflow-hidden border-0 bg-transparent shadow-lg">
              <div className="aspect-4/3 w-full ">
                <img
                  src={image}
                  alt={`Past workshop ${index + 1}`}
                  className="h-full w-full select-none object-cover"
                  loading="lazy"
                  decoding="async"
                  draggable={false}
                />
              </div>
            </div>
          </CarouselItem>
        ))}
      </CarouselContent>
      <CarouselPrevious className="left-2 bg-black/50 text-white hover:bg-black/70 sm:-left-6" />
      <CarouselNext className="right-2 bg-black/50 text-white hover:bg-black/70 sm:-right-6" />
    </Carousel>
  );
}
