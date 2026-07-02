import confused from "@/assets/404/confused_small.webp";
export default function FourohfourPage() {
  return (
    <div className="flex-1 text-white font-jaro items-center justify-center flex-col flex">
      <img src={confused} />
      <span className="txtshdw text-9xl">404</span>
      <span>This page doesnt exist</span>
      <a className="underline" href="/">
        Go back home
      </a>
    </div>
  );
}
