# Adding images to gallery

## Before you do anything

Hi! Before you do anything and upload anything please compress the images. It's a really bad practice to leave 2mb photos in a gallery. Please compress them.
To compress images i highly reccomend a linux machine or a mac with `imagemagick` (installed)[https://imagemagick.org/download/] (homebrew works great with linux)
To compres them first look at what format the images were. In my case they were .png so ill be using that. We will be converting to `.webp` the new and modern format for images that works great with web.
Open your terminal and cd into the folder with images. On some OSes you can right click the folder and open in terminal/console.
In your console type:

```bash
mogrify -format webp -quality 80 *.png
```

This command:

1. Changes format to webp
2. Reduces quality to 80% (un-noticeable)

If your images are in 4k, 3k, 2k or some high resolution you **must** first resize them down!! To do that just add a flag to the command:

```bash
mogrify -resize 50% *.webp
```

You can also do it with sizes like 1920x1080 instead of 50%
50% reduces the image size by 2 times. 33% reduces it by rougly 3 times and so on.
When you've got your images all complete and resized and compressed and converted you can remove the old uncompressed images with a simple

```bash
rm *.png
```

Only now you can proceed to next step.

## Uploading

To add images to the gallery you need to follow first upload them to a cdn of your choice. The one that works best is (https://cdn.hackclub.com)[https://cdn.hackclub.com] because it's from Hack Club.
If you do not have tons of images: upload them by hand
BUT if you have tons of images:

- make yourself an api token for the hc cdn
- run this (vibecoded) script to upload them all:

```bash
#!/usr/bin/env bash
set -euo pipefail

# --- Config ---
CDN_URL="https://cdn.hackclub.com/api/v4/upload"
OUTPUT_FILE="gallery.json"

# --- Ask for inputs (not stored) ---
read -rsp "Enter your CDN API key: " API_KEY
echo
read -rp "Enter folder path to upload: " FOLDER
read -rp "Enter credits text to attach to each image: " CREDITS

if [ ! -d "$FOLDER" ]; then
  echo "Error: '$FOLDER' is not a directory." >&2
  exit 1
fi

# --- Ensure gallery.json exists and is a valid array ---
if [ ! -s "$OUTPUT_FILE" ]; then
  echo "[]" > "$OUTPUT_FILE"
fi

for file in "$FOLDER"/*; do
  [ -f "$file" ] || continue

  filename=$(basename "$file")
  echo "Uploading: $filename"

  response=$(curl -s -X POST \
    -H "Authorization: Bearer $API_KEY" \
    -F "file=@${file}" \
    "$CDN_URL")

  url=$(echo "$response" | jq -r '.url // empty')

  if [ -z "$url" ]; then
    echo "  Failed to upload $filename. Response: $response" >&2
    continue
  fi

  echo "  -> $url"

  # Append new entry to gallery.json using jq
  tmp=$(mktemp)
  jq --arg url "$url" --arg credits "$CREDITS" --arg filename "$filename" \
    '. += [{"url": $url, "credits": $credits, "filename": $filename}]' \
    "$OUTPUT_FILE" > "$tmp" && mv "$tmp" "$OUTPUT_FILE"
done

# Unset API key from memory
unset API_KEY

echo
echo "Done! Appended to $OUTPUT_FILE"
```

It will automatically append credits to it so its fine. If you do not run the script you will need to create `gallery.json` and add the images to it.
And here is how an example `gallery.json` should look like:

```json
[
  {
    "url": "https://cdn.hackclub.com/019ecc7b-4543-789d-ba2d-a6299b798079/IMG_6968.webp",
    "credits": "Beachside Barracoders",
    "filename": "IMG_6968.webp"
  },
  {
    "url": "https://cdn.hackclub.com/019ecc77-a187-7f05-aece-cd91483a98cc/DSCF5709.webp",
    "credits": "Al Salam Community School",
    "filename": "DSCF5709.webp"
  },
  {
    "url": "https://cdn.hackclub.com/019ecc7c-c872-7805-bc8f-2219043f6699/1000088273.webp",
    "credits": "Eastvale Innovators",
    "filename": "1000088273.webp"
  }
]
```

## Changing home images

On the homepage (`src/pages/Home/Home.tsx`) there is a little gallery! To modify it's images you need to open `src/pages/Home/gallery.tsx` and modify the `const images` list
It should look something like this:

```typescript
const images = [
  "https://cdn.hackclub.com/019ecc7c-d7dd-75a7-8add-7d85c6d50e23/1000088286.webp",
  "https://cdn.hackclub.com/019ecc7c-d9c9-7011-856f-544ae9fdd4f3/1000088291.webp",
  "https://cdn.hackclub.com/019ecc7d-58fa-7a1b-8ea1-ff66d13695c5/1000019066.webp",
  "https://cdn.hackclub.com/019ecc7b-65bd-7f07-918a-9f35488bd72f/IMG_9047.webp",
  "https://cdn.hackclub.com/019ecc77-a821-7ffc-b0a7-60777617cc58/DSCF5723.webp",
];
```

## Uploading to site

Open a pr with your changes and merge it! I won't explain this part tho. If you don't know git in terminal (or arent sure of your skills like me) i reccomend using something like Zed's, VSC's git integration or the GitHub desktop app!

## Congrats

If images do not appear check the deployement logs!
