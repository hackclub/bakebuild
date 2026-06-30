import "./home.css";
import cookie1 from "@/assets/home/cookie1.webp";
import cookie2 from "@/assets/home/cookie2.webp";
import stars from "@/assets/home/stars.webp";
import belt from "@/assets/home/belt.webp";
import sticker from "@/assets/home/sticker.webp";
import dimensions from "@/assets/home/dimensions.webp";
import prefed from "@/assets/home/prefed.webp";
import clubCutters from "@/assets/home/clubcutters.webp";
import threeCutters from "@/assets/home/3cutters.webp";
import noHackatime from "@/assets/home/nohackatime.webp";
import guideHere from "@/assets/home/guidehere.webp";
import teachingDino from "@/assets/home/teachingdino.webp";
import bakeit from "@/assets/home/bakeit.webp";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import HomeGallery from "./components/gallery";
import { useState } from "react";

export default function Home() {
  const [step, setStep] = useState<number>(0);
  const steps = [
    <span>
      1. Fill out the form to register your workshop (click button below)
    </span>,
    <span>
      2. Patiently want for a follow-up email saying that your workshop has been
      approved.{" "}
      <span>
        do <u>not</u> start your workshop until you have confirmed that you have
        been approved!!
      </span>{" "}
      Please reach out to{" "}
      <a
        href="https://hackclub.enterprise.slack.com/team/U085US8GYG6"
        className="underline hover:cursor-pointer text-cyan-200"
      >
        @Shaan
      </a>{" "}
      if you have anything time-sensitive, special requests, or any other
      issues.
    </span>,
    <span>
      3. Host your workshop! You can find a guide{" "}
      <a className="underline hover:cursor-pointer text-cyan-200">here</a>. Have
      your participants each fill out the submission form.{" "}
      <b>
        Please have them select that they are participating in a workshop and
        have them select your workshop code in the subsequent question
      </b>
    </span>,
    <span>
      4. Once ALL of your participants have submitted AND are approved, fill out
      the Workshop Complete form you'll receive in your email. You will need to
      track this YOURSELF. Any submissions from your workshop AFTER this form is
      completed will be rejected
    </span>,
    <span>
      5. Wait for the cookie cutters in the mail and host a follow up meeting
      with your cookies! You will receive around $5 USD per approved submission
      from your club at the time. I highly reccomend baking your own cookies and
      using the cookie cutters!!
    </span>,
  ];
  return (
    <div className="flex-1">
      <section
        className="relative text-white border-5 flex flex-col h-screen w-full justify-center overflow-hidden"
        // style={{
        //   backgroundImage: `url("https://i.ibb.co/5hxNqBK1/top-view-paper-texture.jpg")`,
        // }}
      >
        <div className="flex flex-col justify-center items-center">
          <img
            src={cookie1}
            className="absolute w-40 top-10 -right-10 cookie"
            alt="Image of a chocolate chip cookie"
          />
          <img
            src={cookie1}
            className="absolute w-60 bottom-10 -left-20 cookie2"
            alt="Image of a chocolate chip cookie"
          />
          <img
            src={cookie2}
            className="absolute w-50 top-20 -left-20 cookie2"
            alt="Image of a chocolate chip cookie"
          />
          <img
            src={cookie2}
            className="absolute w-50 bottom-19 -right-20 cookie"
            alt="Image of a chocolate chip cookie"
          />
          {/*<img src="src/assets/cut.webp" className="w-250" />*/}
          <h1 className="relative z-5 text-[30vw] lg:text-[280px] xl:text-[30vh] -mt-10 font-jaro uppercase inline-flex flex-col w-fit leading-none">
            <img
              fetchPriority="low"
              src={stars}
              className="absolute invisible sm:visible -top-12 -left-10 -translate-x-1/2 w-100 pointer-events-none"
              alt="Stars"
            />
            <img
              fetchPriority="low"
              src={stars}
              className="absolute invisible sm:visible -bottom-12 -right-10 translate-x-1/2 rotate-180 w-100 pointer-events-none"
              alt="Stars"
            />
            <div className="flex flex-col gap-30">
              <span className="txtshdw z-50 flex w-full text-shadow-accent justify-between">
                <span>B</span>
                <span>A</span>
                <span>K</span>
                <span>E</span>
              </span>

              <span className="txtshdw z-50">BUILD</span>
            </div>
            <img
              src={sticker}
              className="absolute w-75 sm:w-[55vw] lg:w-125 xl:w-[50vh]  left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 pointer-events-none"
              alt="Image of a chocolate chip cookie"
            />
          </h1>

          <span className="txtstroke2 px-5 lg:px-5 text-md text-center z-50 sm:text-2xl lg:text-3xl font-slackey">
            Make your own cookie cutter and get it shipped to you!
          </span>
          <div className="flex gap-3 z-100 mt-2">
            <Tooltip>
              <TooltipTrigger asChild>
                <a
                  href="#make-it"
                  className="underline border-2 px-5 py-2 text-xl rounded-xl font-milkyway"
                >
                  Make It
                </a>
              </TooltipTrigger>
              <TooltipContent>Learn how to make it</TooltipContent>
            </Tooltip>
            <Tooltip>
              <TooltipTrigger asChild>
                <a
                  href="#bake-it"
                  className="underline group border-2 border-dashed px-5 py-2 text-xl rounded-xl font-milkyway"
                >
                  Bake It
                </a>
              </TooltipTrigger>
              <TooltipContent>
                <span>Ship your project!</span>
              </TooltipContent>
            </Tooltip>
          </div>
        </div>
        <div className="belt-container absolute bottom-0">
          <div className="belt-track">
            <div className="belt-group">
              <img
                className="belt-image h-[10vh]"
                src={belt}
                alt="moving belt"
              />
              <img
                className="belt-image h-[10vh]"
                src={belt}
                alt="moving belt"
              />
            </div>
            <div className="belt-group" aria-hidden="true">
              <img
                className="belt-image h-[10vh]"
                src={belt}
                alt="moving belt"
              />
            </div>
          </div>
        </div>
      </section>
      <section
        className="relative border-5 text-white flex flex-col  w-full overflow-hidden"
        id="make-it"
      >
        <div className="justify-center flex flex-col p-10">
          <div className="flex flex-col justify-center items-center">
            <h1 className="smtxt text-6xl sm:text-8xl font-jaro">Make it!</h1>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 lg:gap-4 font-jaro text-2xl mt-5 items-start w-fit mx-auto">
            <span className="flex border p-2 rounded-sm flex-col items-center text-center gap-3 max-w-[290px] mx-auto">
              <img
                src={dimensions}
                className="w-full max-w-50"
                alt="Image representing Maximum 100x100x100mm on each cutter"
              />
              <span>Maximum 100x100x100mm on each cutter</span>
            </span>
            <span className="flex border p-2 rounded-sm flex-col items-center text-center gap-3 max-w-[290px] mx-auto">
              <img
                src={prefed}
                className="w-full max-w-50"
                alt="Image representing Any CAD software accepted, but Onshape preferred!"
              />
              <span>Any CAD software accepted, but Onshape preferred!</span>
            </span>
            <span className="flex border p-2 rounded-sm flex-col items-center text-center gap-3 max-w-[290px] mx-auto sm:col-span-2 lg:col-span-1">
              <img
                src={clubCutters}
                className="w-full max-w-50"
                fetchPriority="low"
                alt="Image representing Up to 3 cutters per person for clubs!"
              />
              <span>Up to 3 cutters per person for clubs!</span>
            </span>
            <span className="flex border p-2 rounded-sm flex-col items-center text-center gap-3 max-w-[290px] mx-auto">
              <img
                src={threeCutters}
                className="w-full max-w-50"
                fetchPriority="low"
                alt="Image representing 3 cutters REQUIRED for individuals"
              />
              <span>3 cutters REQUIRED for individuals</span>
            </span>
            <span className="flex border p-2 rounded-sm flex-col items-center text-center gap-3 max-w-[290px] mx-auto">
              <img
                src={noHackatime}
                className="w-full max-w-30"
                fetchPriority="low"
                alt="Image representing Time tracking not required!"
              />
              <span>Time tracking not required!</span>
            </span>
            <span className=" border p-2 rounded-sm flex flex-col items-center text-center gap-3 max-w-[290px] mx-auto">
              <img
                src={guideHere}
                className="w-full max-w-50"
                alt="Image representing Guide Available!"
              />
              <span>Open guide with buttons below</span>
            </span>
          </div>
          <div className="w-full items-center justify-center gap-3 mt-10 flex not-sm:flex-col">
            <div className="text-xl border-4 p-2 w-fit font-slackey underline">
              <a href="/guide">Open tutorial (on site)</a>
            </div>
            <div className="text-xl border-4 p-2 w-fit font-slackey underline">
              <a href="https://cdn.hackclub.com/019ee9f0-4584-7836-a0b9-316d40a5b0da/bakebuildguidev4.pdf">
                Open presentation file
              </a>
            </div>
          </div>

          {/*<div className="flex flex-col justify-center items-center">
            <span className="text-xl font-jaro mt-5">
              You can print cookie cutters on your own printer too! (no filament
              grants)
            </span>
          </div>*/}
        </div>
      </section>
      <section
        className="relative border-5 text-white flex flex-col  w-full overflow-hidden"
        id="bake-it"
      >
        <div className="justify-center flex flex-col items-center  p-10">
          <div className="flex mb-5 flex-col justify-center items-center">
            <h1 className="smtxt  text-6xl sm:text-8xl font-jaro">Bake it!</h1>
          </div>
          <img src={bakeit} fetchPriority="low" className="lg:max-w-[50vw]" />
          <p className="text-xl mt-3 font-jaro max-w-2xl">
            You can print your own cookie cutters on your own 3d printer (no
            filament grants tho) or you may request them to be shipped to your
            house from Hack Club hq
          </p>
          <div className="text-xl mt-5 text-center font-slackey underline">
            <a href="https://hack.club/submit-bakebuild">
              Submit your project here
            </a>
          </div>
        </div>
      </section>

      <section className="relative border-5 text-white xl:px-50 2xl:px-80 flex flex-col bg-red-400  w-full overflow-hidden">
        <div className="justify-center items-center flex flex-col p-10">
          <div className="flex flex-col justify-center items-center">
            <h1 className="smtxt text-6xl sm:text-8xl font-jaro">
              Run a workshop!
            </h1>
          </div>
          <img
            src={teachingDino}
            fetchPriority="low"
            alt="Orpheus dino in a classroom saying BakeBuild"
          />
          <div className="max-w-3xl flex-col flex items-start text-left">
            <span className="text-3xl font-jaro">Hey club leader!</span>
            <p className="text-lg text-left justify-left items-left flex flex-col">
              <span className="text-left w-full">
                Have you ever wanted to run a BakeBuild workshop in your club?
                Yes? Everyone wants that. It's actually pretty easy! To run a
                workshop you <b>must</b> have first completed BakeBuild. There
                are a couple steps to organising one:
              </span>
              <div className="p-2 border-2 mt-3 flex">
                <span className="mr-2">{steps.at(step)} </span>
                <button
                  className="ml-auto text-sm font-slackey border-l-2 pl-2 underline"
                  onClick={() =>
                    setStep(step !== steps.length - 1 ? step + 1 : 0)
                  }
                >
                  Next step
                </button>
              </div>
              <div className="flex gap-2 mt-2">
                {steps.map((_, index) => (
                  <div
                    className="rounded-full w-3 h-3"
                    style={
                      step === index
                        ? { backgroundColor: "white" }
                        : { backgroundColor: "#D8D8D8" }
                    }
                  ></div>
                ))}
              </div>
              {/*<a
                className="underline"
                href="http://hack.club/bakebuild-workshop"
              >
                Open the tutorial & submission form &#40;click&#41;.
              </a>{" "}*/}
              <br />
            </p>
            <a
              href="http://hack.club/bakebuild-workshop"
              className=" border-4 p-2 w-fit font-slackey hover:cursor-pointer underline"
            >
              Open workshop form
            </a>
          </div>
        </div>
      </section>
      <section className="relative border-5 text-white xl:px-50 2xl:px-80 flex flex-col  w-full overflow-hidden">
        <div className="justify-center flex flex-col p-10">
          <div className="flex flex-col justify-center items-center">
            <h1 className="smtxt text-6xl sm:text-8xl font-jaro">
              Past workshops
            </h1>
            <div className="flex flex-col justify-center items-center text-center mt-5">
              <HomeGallery />

              <div className="flex flex-col justify-center align-center text-center mt-5">
                <a
                  className=" border-4 w-fit p-2 mt-3 font-slackey hover:cursor-pointer underline"
                  href="/gallery"
                >
                  View more images in the gallery
                </a>
                <span className="text-xs mt-1">Credits on /gallery page</span>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
