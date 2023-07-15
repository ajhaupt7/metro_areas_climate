import { DateTime } from 'luxon';
import { writeFile, readdir, readFile } from 'node:fs/promises';
import geojson from './topMetroGeojson.json' assert { type: "json" };
import path from 'node:path';
import { Feature, Point, Properties, centerOfMass } from '@turf/turf';
import { createObjectCsvWriter } from 'csv-writer';

interface OpenMeteoRes {
  lat: number;
  lng: number;
  generationtime_ms: number;
  utc_offset_seconds: number;
  timezone: string;
  timezone_abbreviation: string;
  elevation: number;
  daily_units: {
    time: string;
    temperature_2m_max: string;
    temperature_2m_min: string;
    temperature_2m_mean: string;
    rain_sum: string;
    snowfall_sum: string;
    windspeed_10m_max: string;
  };
  daily: {
    time: string[];
    temperature_2m_max: number[];
    temperature_2m_min: number[];
    temperature_2m_mean: number[];
    rain_sum: number[];
    snowfall_sum: number[];
    windspeed_10m_max: number[];
  }
}

interface Day {
  date: DateTime;
  maxTemp: number;
  minTemp: number;
  avgTemp: number;
  rain: number;
  snow: number;
  avgWind: number;
}

interface Month {
  maxTemp: number;
  minTemp: number;
  avgTemp: number;
  rain: number;
  snow: number;
  avgWind: number;
  // Tracking these for the average
  avgTempSum?: number;
  windSum?: number;
}

const jsonDirPath = './data'; // Replace this with your actual directory path

function getMonth(date: DateTime) {
  return `${date.monthShort}-${date.year}`;
}

function getRequest(center: Feature<Point, Properties>) {
  return `https://archive-api.open-meteo.com/v1/archive?latitude=${center.geometry.coordinates[1]}&longitude=${center.geometry.coordinates[0]}&start_date=1950-01-01&end_date=2023-06-30&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,rain_sum,snowfall_sum,windspeed_10m_max&temperature_unit=fahrenheit&windspeed_unit=mph&precipitation_unit=inch&timezone=America%2FLos_Angeles`;
}

async function fetchAreaData(areaName: string) {
  const area = geojson.find(f => f.properties.NAME === areaName);

  if (!area) {
    throw new Error(`Could not find area ${areaName} in the FeatureCollection.`);
  }

  const center = centerOfMass(area);
  const res = await fetch(getRequest(center));
  const json: OpenMeteoRes = await res.json();

  const formatted: Day[] = [];

  json.daily.time.forEach((date, i) => {
    const obj = {
      date: DateTime.fromISO(date),
      maxTemp: json.daily.temperature_2m_max[i],
      minTemp: json.daily.temperature_2m_min[i],
      avgTemp: json.daily.temperature_2m_mean[i],
      rain: json.daily.rain_sum[i],
      snow: json.daily.snowfall_sum[i],
      avgWind: json.daily.windspeed_10m_max[i]
    };

    formatted.push(obj);
  });

  const months = formatted.reduce((acc, curr, i) => {
    const totalDaysThusFar = curr.date.day;
    const month = getMonth(curr.date);
    const monthObj = acc[month];

    if (!monthObj) {
      acc[month] = curr;
    } else {
      const newMonthObj = {
        maxTemp: Math.max(curr.maxTemp, monthObj.maxTemp),
        minTemp: Math.min(curr.minTemp, monthObj.minTemp),
        avgTemp: 0,
        avgWind: 0,
        rain: curr.rain + monthObj.rain,
        snow: curr.snow + monthObj.snow,
        avgTempSum: curr.avgTemp + (monthObj.avgTempSum ?? 0),
        windSum: curr.avgWind + (monthObj.windSum ?? 0)
      };

      acc[month] = newMonthObj;
    }

    const nextDay = formatted[i + 1]
    const nextDayIsNextMonth = !nextDay || nextDay.date.monthShort !== curr.date.monthShort;

    if (nextDayIsNextMonth) {
      acc[month].avgTemp = acc[month].avgTempSum! / totalDaysThusFar;
      acc[month].avgWind = acc[month].windSum! / totalDaysThusFar;
    }

    return acc;
  }, {} as Record<string, Month>); 

  const finalFormat = {
    areaName,
    geojson: area,
    center,
    months,
    elevation: json.elevation,
    coords: [json.lat, json.lng],
  }

  await writeFile(path.join(process.cwd(), `data/${areaName.replace('/', '_')}.json`), JSON.stringify(finalFormat));
  console.log(`COMPLETED ${areaName}`)
}

async function getAllAreas() {
  await Promise.all(geojson.map(async f => await fetchAreaData(f.properties.NAME)));
  console.log("SUCCESS");
} 

async function readJsonFilesFromDirectory(directoryPath: string) {
  const files = await readdir(directoryPath);
  const jsonFiles = files.filter((file) => path.extname(file).toLowerCase() === '.json');

  return jsonFiles.map((file) => path.join(directoryPath, file));
}

// Function to convert a JSON file to CSV format
async function jsonToCsv(jsonFilePath: string) {
  try {
    const file = await readFile(jsonFilePath, { encoding: 'utf8' });
    const jsonData = JSON.parse(file);

    const csvWriter = createObjectCsvWriter({
      path: jsonFilePath.replace('.json', '.csv'),
      header: [
        { id: 'month', title: 'month'},
        { id: 'maxTemp', title: 'maxTemp' }, 
        { id: 'minTemp', title: 'minTemp' }, 
        { id: 'avgWind', title: 'avgWind' }, 
        { id: 'rain', title: 'rain' }, 
        { id: 'snow', title: 'snow' }, 
      ],
    });

    return csvWriter.writeRecords(Object.keys(jsonData.months).map(key => {
      const month = jsonData.months[key];
      return {
        month: key,
        maxTemp: month.maxTemp,
        minTemp: month.minTemp,
        avgTemp: month.avgTemp,
        avgWind: month.avgWind,
        rain: month.rain,
        snow: month.snow,
      }
    }));
  } catch (err: any) {
    console.error(`Error processing ${jsonFilePath}: ${err.message}`);
  }
}

async function convertToCsv() {
  // Read all JSON files from the directory and convert each to CSV
  const jsonFiles = await readJsonFilesFromDirectory(jsonDirPath);

  await Promise.all(jsonFiles.map(async (jsonFile) => {
    await jsonToCsv(jsonFile);
  }));
}

convertToCsv();