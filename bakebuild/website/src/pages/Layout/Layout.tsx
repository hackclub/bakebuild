import { Outlet } from "react-router-dom";

export default function Layout() {
  return (
    <div className="flex flex-col min-h-screen">
      <div className="absolute left-5 z-100">
        <img
          fetchPriority="low"
          src="https://assets.hackclub.com/flag-orpheus-top.svg"
          className="w-[25vw] sm:w-[30vw] md:w-50"
          alt="Hack Club mascot orpheus holding a red flag saying 'Hack Club' with white text"
        />
      </div>
      <nav className="absolute right-2 z-100 p-3 text-white ">
        <div className="flex gap-3 text-right ">
          <a
            className="text-right font-jaro bg-gray-900 py-1 px-2 text-xl"
            href="/"
          >
            Home
          </a>
          <a
            className="text-right font-jaro bg-gray-900 py-1 px-2 text-xl"
            href="/gallery"
          >
            Gallery
          </a>
          <a
            className="text-right font-jaro bg-gray-900 py-1 px-2 text-xl"
            href="/guide"
          >
            Guide
          </a>
        </div>
      </nav>
      <main className="flex flex-1 flex-col">
        <Outlet />
      </main>
      {/*TODO: */}
      <footer className="bg-black text-center justify-center text-white font-jaro text-md p-4 flex">
        <span>
          Made by teens, for teens at{" "}
          <a className="underline text-sky-400" href="https://hackclub.com">
            Hack Club
          </a>
          , 18 and under only, check out our{" "}
          <a
            className="underline text-sky-400"
            href="https://github.com/hackclub/bakebuild"
          >
            GitHub
          </a>
          ,{" "}
          <a
            className="underline text-sky-400"
            href="https://hcb.hackclub.com/bakebuild"
          >
            HCB
          </a>
          , and the{" "}
          <a
            className="underline text-sky-400"
            href="https://hackclub.com/slack/"
          >
            Slack.
          </a>
        </span>
        <div className="border mx-1 border-white"></div>
        <a
          href="https://jams.hackclub.com/jam/bakebuild"
          className="underline text-sky-400"
        >
          {" "}
          Open Jam
        </a>
      </footer>
    </div>
  );
}
