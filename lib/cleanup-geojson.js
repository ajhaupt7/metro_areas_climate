import { metroAreas } from './metroAreas.js';
import geojson from './geojson.json' assert { type: "json" };
import { writeFile } from 'node:fs/promises';
 
async function main() {
  try {
    const features = geojson.features.filter((feature) => {
      return metroAreas.find(a => a.includes(feature.properties.NAME ?? ''));
    });

    await writeFile('./res.json', JSON.stringify(features));
    console.log("SUCCESS");
  } catch (e) {
    console.error(e);
  }
}

main();