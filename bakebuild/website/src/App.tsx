import { BrowserRouter, Routes, Route } from "react-router-dom";
import Layout from "@/pages/Layout/Layout";
import Home from "@/pages/Home/Home";
import { TooltipProvider } from "@/components/ui/tooltip";
import GalleryPage from "@/pages/Gallery/GalleryPage";
import GuidePage from "@/pages/Guide/GuidePage";
export function App() {
  return (
    <BrowserRouter>
      <TooltipProvider>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<Home />} />
            <Route path="/gallery" element={<GalleryPage />} />
            <Route path="/guide" element={<GuidePage />} />
          </Route>
        </Routes>
      </TooltipProvider>
    </BrowserRouter>
  );
}

export default App;
