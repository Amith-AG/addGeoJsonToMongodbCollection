import { MongoClient } from "mongodb";
import dotenv from "dotenv";

dotenv.config();

const uri = process.env.MONGO_URI;
let client;
try {
  client = new MongoClient(uri);
  await client.connect();
} catch (error) {
  console.error("Failed to connect to MongoDB:", error);
  process.exit(1);
}

let skip = 0;
let batch = 50;
let count = 1;
let hasNext = true;

const batchSourceCollectionFetcher = async () => {
  try {
    const sourceDb = client.db(process.env.DATABASE_NAME);
    const sourceCollection = sourceDb.collection(
      process.env.SOURCE_COLLECTION_NAME
    );

    while (hasNext) {
      const sourceCollectionDataArray = await sourceCollection
        .find()
        .skip(skip)
        .limit(batch)
        .toArray();
        
      if (sourceCollectionDataArray.length === 0) {
        hasNext = false;
        console.log("---------No more data to fetch--------");
        break;
      }

      const recordList = [];
      // Process records in sequence rather than parallel
      for (const data of sourceCollectionDataArray) {
        try {
          console.log("count:", count);
          const placeDetails = await getPlaceDetails(data);
          if (placeDetails) {
            const { lat, lng, place_id } = placeDetails;
            recordList.push({
              city: `${data.city}, ${data.state}`,
              state: data.state,
              zipcode: data.zipcode,
              zone: data.zone,
              fa_station: data.fa_station,
              country: data.country,
              place_id,
              location: {
                type: "Point",
                coordinates: [lng, lat],
              },
            });
          }
          count++;
        } catch (error) {
          console.error(`Failed to process record ${count}:`, error);
        }
      }

      if (recordList.length > 0) {
        await uploadDataToTargetCollection(recordList);
      }

      skip += batch;
    }
  } catch (error) {
    console.error("Batch processing error:", error);
    throw error; // Propagate the error
  }
};

const getPlaceDetails = async (data) => {
  const maxRetries = 3;
  const delayMs = 300; // 1 second delay between requests
  const { city, state, country, zipcode, fa_station, zone } = data;
  const apiUrl = `https://maps.googleapis.com/maps/api/geocode/json?address=${city}&components=country:${country.toLowerCase()}&key=${
    process.env.GOOGLE_MAPS_API_KEY
  }`;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await new Promise((resolve) => setTimeout(resolve, delayMs));

      const placeResponse = await fetch(apiUrl, {
        timeout: 2000, // 2 second timeout
      });

      if (!placeResponse.ok) {
        throw new Error(`HTTP error! status: ${placeResponse.status}`);
      }

      const placeResponseData = await placeResponse.json();

      if (
        !placeResponseData.results ||
        placeResponseData.results.length === 0
      ) {
        throw new Error("No results found for location");
      }

      const results = placeResponseData.results[0];
      const { geometry, place_id } = results;
      const { lat, lng } = geometry.location;
      console.log(`city:${city},place_id:${place_id}`);
      return { lat, lng, place_id };
    } catch (error) {
      if (attempt === maxRetries) {
        console.error(
          `Failed after ${maxRetries} attempts for city: ${data.city}`,
          error
        );
        throw error;
      }
      console.warn(
        `Attempt ${attempt} failed for city: ${data.city}, retrying...`
      );
    }
  }
};

const uploadDataToTargetCollection = async (data) => {
  try {
    console.log("---------Uploading data to target collection--------");
    const targetDb = client.db(process.env.DATABASE_NAME);
    const targetCollection = targetDb.collection(
      process.env.TARGET_COLLECTION_NAME
    );
    await targetCollection.insertMany(data);
    console.log("---------Data uploaded to target collection--------");
  } catch (error) {
    console.error("Upload error:", error);
    throw error;
  }
};

// Main execution
try {
  await batchSourceCollectionFetcher();
} catch (error) {
  console.error("Application error:", error);
} finally {
  if (client) {
    await client.close();
  }
  process.exit(0);
}
