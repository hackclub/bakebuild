import { Step, StepHeader, StepContent } from "./components/reproducible-step";
import cadtocookies from "@/assets/guide/cadtocookies.webp";
import gettingStarted from "@/assets/guide/gettingstarted.webp";
import dinoOutline from "@/assets/guide/dinooutline.webp";
import importingTheImage from "@/assets/guide/importingtheimage.webp";
import insertingTheImage from "@/assets/guide/insertingtheimage.webp";
import insertingTheImage2 from "@/assets/guide/insertingtheimage2.webp";
import dimension from "@/assets/guide/dimension.webp";
import tracing from "@/assets/guide/tracing.webp";
import threeDTime from "@/assets/guide/3dtime.webp";
import prettylittleborder from "@/assets/guide/prettylittleborder.webp";
import admire from "@/assets/guide/admire.webp";
import screenshot from "@/assets/guide/screenshot.webp";
import exportImage from "@/assets/guide/export.webp";
import exportImage2 from "@/assets/guide/export2.webp";
import doubleCheck from "@/assets/guide/doublecheck.webp";
import "./guidepage.css";
export default function GuidePage() {
  return (
    <div className="justify-center flex text-white align-center">
      <div className="mt-10 px-1 w-screen xl:w-3/5">
        <div className="justify-center text-left flex flex-col p-10">
          <div className="flex flex-col justify-center items-center">
            <h1 className="smtxt text-8xl font-jaro">The Guide</h1>
            <div className="text-xl font-slackey mt-3">
              <span>Welcome! Welcome to the ultimate guide page.</span>
            </div>
          </div>
        </div>
        {/*<span>content</span>*/}
        <div className="flex flex-col gap-5">
          <Step className="">
            <StepHeader>The Goal</StepHeader>
            <StepContent>
              <div className="">
                <h3>You Will...</h3>
                <ul className="">
                  <li>
                    Learn the basics of CAD by designing your own cookie cutter
                  </li>
                  <li>Get your cookie cutter printed out</li>
                  <li className="font-bold text-lg">Get cookies!!!!</li>
                </ul>
              </div>
              <img className="guideimage" src={cadtocookies} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Getting Started</StepHeader>
            <StepContent>
              <div className="">
                <ul className="">
                  <li>First, log in or sign up to Onshape</li>
                  <li className="">
                    <span>Once signed in</span>
                    <ul className="pl-4">
                      <li>Click "Create"</li>
                      <li>Click "Document"</li>
                      <li>Name it "Cookie Cutter"</li>
                    </ul>
                  </li>
                </ul>
              </div>
              <img className="guideimage" src={gettingStarted} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Design Time</StepHeader>
            <StepContent>
              <div className="">
                <ul>
                  <li>
                    <span>Decide what you want to make</span>
                    <ul className="pl-4">
                      <li>Talk to your friends and brainstorm ideas</li>
                      <li>
                        Sketch an outline of it using either digital tools or
                        paper
                      </li>
                    </ul>
                  </li>
                  <li>
                    Make sure it has a simple outline, like this loch ness
                    monster!
                  </li>
                  <li>
                    Once you've got your outline, save an image of it and return
                    to Onshape
                  </li>
                </ul>
              </div>
              <img className="guideimage" src={dinoOutline} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Importing The Image</StepHeader>
            <StepContent>
              <div className="">
                <ul>
                  <li>
                    Now that you have your template, we can start designing!
                  </li>
                  <li>
                    <span>
                      Click the Plus Sign next to Part Studio and dropdown menu
                      should appear (bottom left corner){" "}
                      <ul className="pl-4">
                        <li>Click on the last option "Import"</li>
                      </ul>
                    </span>
                  </li>
                  <li>Select your chosen file and click "Open"</li>
                  <li>Message will confirm when the image has been imported</li>
                </ul>
              </div>
              <img className="guideimage" src={importingTheImage} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Inserting The Image</StepHeader>
            <StepContent>
              <div className="">
                <ul>
                  <li>Click "Sketch" in the top left</li>
                  <li>
                    <span>
                      Click the "Top" plane{" "}
                      <ul className="ml-4">
                        <li>
                          Might help to change your POV by clicking the top side
                          of the cube in the top right
                        </li>
                        <li>"Top plane" should appear under "Sketch plane"</li>
                      </ul>
                    </span>
                  </li>
                  <li>
                    Find the folder icon (top middle) and click the drop down
                  </li>
                  <li>Press "Insert image"</li>
                </ul>
              </div>
              <img className="guideimage" src={insertingTheImage} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Inserting The Image</StepHeader>
            <StepContent>
              <div className="">
                <ul>
                  <li>Choose your image from the pop-up</li>
                  <li>A message should appear saying "Draw image rectangle"</li>
                  <li>Click and drag to insert the image</li>
                  <li className="underline">DO NOT CONFIRM YOUR SKETCH YET</li>
                </ul>
              </div>
              <img className="guideimage" src={insertingTheImage2} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Dimension</StepHeader>
            <StepContent>
              <ul>
                <li>Click the "Dimension" tool or press "d"</li>
                <li>Click any 2 corners</li>
                <li>
                  Drag your mouse to the outside of the border, and lines and a
                  number should appear, then click again
                </li>
                <li>
                  <span>The number is the size of your image</span>
                  <ul>
                    <li>
                      In general your outline probably shouldn't be over 4in x
                      4in or 100mm x 100mm
                    </li>
                  </ul>
                </li>
              </ul>
              <img className="guideimage" src={dimension} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Tracing</StepHeader>
            <StepContent>
              <ul>
                <li>
                  Once you have your image inserted and properly dimensioned you
                  can starts tracing! DO NOT PRESS THE GREEN CHECKMARK YET!!
                </li>
                <li>
                  <span>
                    Use the spline tool and click along your outline to trace it
                    out
                  </span>
                  <ul className="ml-4">
                    <li>Tip: Use the middle mouse button to navigate around</li>
                    <li>
                      Note that you can edit the point of your spline after
                      confirming it. DO NOT TRY UNDOING WHIE MAKING THE
                      OUTLINE!!!!
                    </li>
                  </ul>
                </li>
                <li>
                  Once you're happy with your outline, confirm your sketch by
                  clicking on the green checkmark
                </li>
              </ul>
              <img className="guideimage" src={tracing} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>3D Time</StepHeader>
            <StepContent>
              <ul>
                <li>
                  Once your sketch is confirmed click the Extrude tool and
                  choose "Thin" (extrude is to the right of "Sketch")
                </li>
                <li>Then, select the outline of your sketch</li>
              </ul>
              <img className="guideimage" src={threeDTime} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>3D Time 2</StepHeader>
            <StepContent>
              <ul>
                <li>
                  Now you can change the thickness <br />{" "}
                  <ul className="ml-4">
                    <li>I find that 2mm/.08in works best</li>
                  </ul>
                </li>
                <li>
                  Then, choose the depth{" "}
                  <ul className="ml-4">
                    <li>You're going to want this to be about 12.7mm/0.5in</li>
                  </ul>
                </li>
                <li>Confirm the extrude</li>
              </ul>
              <img className="guideimage" src={threeDTime} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Pretty Little Border</StepHeader>
            <StepContent>
              <ul>
                <li>Click "Extrude" again</li>
                <li>
                  <span>
                    Select the BOTTOM side of the extrude and use thin to make a
                    pretty border
                  </span>
                  <ul className="pl-4">
                    <li>Tip: Use right click to rotate</li>
                    <li>Thickness ~5mm</li>
                    <li>Depth ~2mm</li>
                  </ul>
                </li>
              </ul>
              <img className="guideimage" src={prettylittleborder} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Admire</StepHeader>
            <StepContent>
              <ul>
                <li>Look at your finished design!</li>
                <li>Isn't it pretty?</li>
                <li>Feel happy and proud!!</li>
              </ul>
              <img className="guideimage" src={admire} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Screenshot</StepHeader>
            <StepContent>
              <ul>
                <li>
                  Take a screenshot of your design (like so) and save it to your
                  computer
                </li>
                <li className="underline">
                  You will need this for submission!!
                </li>
              </ul>
              <img className="guideimage" src={screenshot} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Export</StepHeader>
            <StepContent>
              <ul>
                <li>Now it's time to export it!</li>
                <li>Right click the "Part Studio" tab in the bottom left</li>
                <li>Select "Export"</li>
              </ul>
              <img className="guideimage" src={exportImage} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Export</StepHeader>
            <StepContent>
              <ul>
                <li>A pop up will appear</li>
                <li>
                  Name your file something relevant and boring like "Cookie
                  Cutter" again
                </li>
                <li>Select "STEP" for the format</li>
                <li>Click the blue export button</li>
                <li>
                  You should now have a STEP file of your cookie cutter saved to
                  your computer
                </li>
              </ul>
              <img className="guideimage" src={exportImage2} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Double Checking</StepHeader>
            <StepContent>
              <ul>
                <li>
                  Visit{" "}
                  <a
                    href="https://3dviewer.net"
                    className="underline hover:cursor-pointer"
                  >
                    3dviewer.net
                  </a>
                </li>
                <li>
                  560 Upload your file to the site (you can drag the entire zip)
                </li>
                <li>
                  Once uploaded make sure the XYZ sizes don't exceed 100mm
                </li>
                <li>
                  <span>
                    If it exceeds 100mm you can reshape the file with the help
                    from{" "}
                    <a
                      href="https://www.youtube.com/watch?v=_cFTrdposH0"
                      className="underline"
                    >
                      this video
                    </a>
                  </span>
                </li>
              </ul>

              <img className="guideimage" src={doubleCheck} />
            </StepContent>
          </Step>
          <Step>
            <StepHeader>Submitting</StepHeader>
            <StepContent>
              <div className="">
                <span>
                  Finally your hard work paid off and you can submit your cookie
                  cutters!!
                </span>
                <ul>
                  <li>
                    Go to the <a href="/">Homepage</a>
                  </li>
                  <li>Click the "Bake It!" button</li>
                  <li>Fill out the form</li>
                  <li>That's all!</li>
                  <li>
                    <a
                      className="underline"
                      href="https://forms.hackclub.com/t/cecBQMan6Jus"
                    >
                      Or alternatively click me
                    </a>
                  </li>
                </ul>
              </div>
            </StepContent>
          </Step>
          <div className="p-5 border-4 flex flex-col">
            <span className="text-4xl font-jaro flex gap-2 align-center items-center">
              BYE!{" "}
              <img
                className="guideimage"
                src="https://emoji.slack-edge.com/T09V59WQY1E/wavey/5bd2842c38fedd5c.gif"
              />
            </span>
            <span>Thanks for following the guide we hope you liked it!</span>
          </div>
        </div>
      </div>
    </div>
  );
}
