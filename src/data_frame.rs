use csv::{ReaderBuilder, StringRecord, WriterBuilder};

use futures::future::join_all;
use tokio::sync::Semaphore;
use std::path::Path;
use std::sync::Mutex;
use indicatif::ProgressBar;
use reqwest::Client;
use std::iter::Iterator;
use std::error::Error;
use std::sync::Arc;
use serde_json::Value;
use std::fmt::{Formatter, Display};
use std::time::Duration;


#[derive(Default, Clone)]
pub struct DataFrame {
    path: String,
    headers: Vec<String>,
    pub shape: (usize, usize),
    delimiter: char,
    pub prefix: String,

    // The DATA
    data: Vec<Vec<String>>,

    // Indexes for matching, fetching, etc (index for data)
    id: Option<usize>,
    addr1: Option<usize>,
    addr2: Option<usize>,
    city: Option<usize>,
    state: Option<usize>,
    zipcode: Option<usize>,

    // Columns (because lat and lng have different type) Excluded from headers
    lat: Option<Vec<f64>>,
    lng: Option<Vec<f64>>,

    // Additional Output columns
    pub output_cols: Vec<usize>,
    compare_cols: Vec<usize>
}

impl Display for DataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{{")?;
        writeln!(f, "\tpath:\t{}", self.path)?;
        writeln!(f, "\tprefix:\t{}\n", self.prefix)?;

        writeln!(f, "\taddr1:\t\t{}", self.addr1.map_or("None".to_string(), |e| e.to_string()))?;
        writeln!(f, "\taddr2:\t\t{}", self.addr2.map_or("None".to_string(), |e| e.to_string()))?;
        writeln!(f, "\tcity:\t\t{}", self.city.map_or("None".to_string(), |e| e.to_string()))?;
        writeln!(f, "\tstate:\t\t{}", self.state.map_or("None".to_string(), |e| e.to_string()))?;
        writeln!(f, "\tzipcode:\t{}\n", self.zipcode.map_or("None".to_string(), |e| e.to_string()))?;

        writeln!(f, "\tlat:\t{}", self.lat.as_ref().map_or("Not Found", |_| "Found"))?;
        writeln!(f, "\tlng:\t{}\n", self.lng.as_ref().map_or("Not Found", |_| "Found"))?;

        writeln!(f, "\toutput_cols: {{")?;
        for col in self.output_cols.iter() {
            writeln!(f, "\t\t{}", self.headers[*col])?;
        }
        writeln!(f, "\t}}")?;

        writeln!(f, "\tcompare_cols: {{")?;
        for col in self.compare_cols.iter() {
            writeln!(f, "\t\t{}", self.headers[*col])?;
        }
        writeln!(f, "\t}}")?;

        writeln!(f, "}}")?;
        Ok(())
    }
}

impl DataFrame {
    // CONSTRUCTORS
    pub fn from_path(path: &str) -> DataFrame {
        // Try to guess delimiter based on number of headers returned
        let comma_count = {
            let mut reader = ReaderBuilder::new()
                .delimiter(b',')
                .from_path(path)
                .unwrap();

            reader.headers().unwrap().iter().count()
        };

        let pipe_count = {
            let mut reader = ReaderBuilder::new()
                .delimiter(b'|')
                .from_path(path)
                .unwrap();

            reader.headers().unwrap().iter().count()
        };

        let delimiter = if pipe_count > comma_count {'|'} else {','};

        // Read in the file for further analysis
        let (mut headers, width, height) = {
            let mut reader = ReaderBuilder::new()
                .delimiter(delimiter as u8)
                .from_path(path)
                .unwrap();

            // Get headers and size information
            let headers = reader.headers().unwrap()
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>();

            let width = headers.len();
            let height = reader.records().count();

            (headers, width, height)
        };

        // Map headers to special column values
        let mut id = None;
        let mut addr1 = None;
        let mut addr2 = None;
        let mut city = None;
        let mut state = None;
        let mut zipcode = None;
        let mut lat = None;
        let mut lng = None;

        for (index, header) in headers.iter().enumerate() {
            let lower = header.to_lowercase();
            let trim = lower.trim();
            let match_str = trim.replace(" ", "");
            match match_str.as_str() {
                "id" => {
                    id = Some(index);
                }
                "addr1" | "address" | "addr" => {
                    addr1 = Some(index);
                }
                "addr2" | "address2" => {
                    addr2 = Some(index);
                }
                "city" => {
                    city = Some(index);
                }
                "state" => {
                    state = Some(index);
                }
                "zipcode" | "zip" | "postalcode" => {
                    zipcode = Some(index);
                }
                "lat" | "latitude" => {
                    lat = Some(index);
                }
                "lng" | "longitude" => {
                    lng = Some(index);
                }
                _ => {}
            }
        }

        // Modify headers removing lat and lng columns

        if lat.is_some() && lng.is_some() {
            let lat = lat.unwrap();
            let lng = lng.unwrap();

            if lat > lng {
                headers.remove(lng);
                headers.remove(lat-1);
            } else {
                headers.remove(lat);
                headers.remove(lng-1);
            }
        } else if let Some(index) = lat {
            headers.remove(index);
        } else if let Some(index) = lng {
            headers.remove(index);
        }

        // Create empty data vec with capacity for data
        let mut data = Vec::with_capacity(headers.len());
        for _ in 0..headers.len() {
            data.push(Vec::with_capacity(height));
        }

        let mut data_frame = DataFrame {
            path: path.to_string(),
            headers,
            shape: (width, height),
            delimiter,
            id,
            addr1,
            addr2,
            city,
            state,
            zipcode,
            data,
            ..DataFrame::default()
        };

        // Read all data into dataframe
        let mut reader = ReaderBuilder::new()
            .delimiter(delimiter as u8)
            .from_path(path)
            .unwrap();

        // Create vectors for lat/lng if needed
        if lat.is_some() {
            data_frame.lat = Some(Vec::with_capacity(data_frame.shape.1));
        }

        if lng.is_some() {
            data_frame.lng = Some(Vec::with_capacity(data_frame.shape.1));
        }

        // Add all data to correct vector
        for record in reader.records() {
            let mut offset=0;
            let record = record.unwrap();
            for (col, item) in record.iter().enumerate() {
                if lat.is_some() && col==lat.unwrap() {
                    data_frame.lat.as_mut().unwrap().push(item.parse::<f64>().unwrap_or(f64::NAN));
                    offset += 1;
                } else if lng.is_some() && col==lng.unwrap() {
                    data_frame.lng.as_mut().unwrap().push(item.parse::<f64>().unwrap_or(f64::NAN));
                    offset += 1;
                } else {
                    data_frame.data[col-offset].push(item.to_string());
                }
            }
        }

        data_frame
    }

    pub fn with_capacity(width: usize, height: usize) -> DataFrame {
        let mut data = Vec::with_capacity(width);
        for _ in 0..width {
            data.push(Vec::with_capacity(height));
        }
        DataFrame {
            data,
            lat: Some(Vec::with_capacity(height)),
            lng: Some(Vec::with_capacity(height)),
            ..DataFrame::default()
        }
    }

    fn get_col_index(&self, col: &str) -> Result<usize, Box<dyn Error>> {
        let col_option = self.headers.iter()
            .enumerate()
            .find(|e| e.1.eq(col));

        if let Some((index, _)) = col_option {
            Ok(index)
        } else {
            return Err(format!("No column named {}", col))?;
        }
    }

    // BOOLEAN CHECKS
    pub fn ready_to_fetch(&self) -> bool {
        self.addr1.is_some() &&
        self.city.is_some() &&
        self.state.is_some()
    }

    pub fn ready_to_match(&self) -> bool {
        self.lat.is_some() &&
        self.lng.is_some()
    }

    // GETTERS
    pub fn get_headers(&self) -> &Vec<String> {
        &self.headers
    }

    pub fn set_headers(&mut self, headers: Vec<String>) {
        self.headers = headers;
    }

    // Special Columns
    pub fn id(&self) -> Option<&Vec<String>> {
        if self.id.is_none() {return None;}
        Some(&self.data[self.id.unwrap()])
    }

    pub fn addr1(&self) -> Option<&Vec<String>> {
        if self.addr1.is_none() {return None;}
        Some(&self.data[self.addr1.unwrap()])
    }

    pub fn addr2(&self) -> Option<&Vec<String>> {
        if self.addr2.is_none() {return None;}
        Some(&self.data[self.addr2.unwrap()])
    }

    pub fn city(&self) -> Option<&Vec<String>> {
        if self.city.is_none() {return None;}
        Some(&self.data[self.city.unwrap()])
    }

    pub fn state(&self) -> Option<&Vec<String>> {
        if self.state.is_none() {return None;}
        Some(&self.data[self.state.unwrap()])
    }

    pub fn zipcode(&self) -> Option<&Vec<String>> {
        if self.zipcode.is_none() {return None;}
        Some(&self.data[self.zipcode.unwrap()])
    }

    pub fn lat(&self) -> Option<&Vec<f64>> {
        if self.lat.is_none() {return None;}
        Some(self.lat.as_ref().unwrap())
    }

    pub fn lat_mut(&mut self) -> Option<&mut Vec<f64>> {
        if self.lat.is_none() {return None;}
        Some(self.lat.as_mut().unwrap())
    }

    pub fn lng(&self) -> Option<&Vec<f64>> {
        if self.lng.is_none() {return None;}
        Some(self.lng.as_ref().unwrap())
    }

    pub fn lng_mut(&mut self) ->Option<&mut Vec<f64>> {
        if self.lng.is_none() {return None;}
        Some(self.lng.as_mut().unwrap())
    }

    pub fn data(&self) -> &Vec<Vec<String>> {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut Vec<Vec<String>> {
        &mut self.data
    }

    // SETTERS
    pub fn add_output_column(&mut self, col: &str) -> Result<(), Box<dyn Error>> {
        self.output_cols.push(self.get_col_index(col)?);
        Ok(())
    }

    pub fn add_compare_column(&mut self, col: &str) -> Result<(), Box<dyn Error>> {
        self.compare_cols.push(self.get_col_index(col)?);
        Ok(())
    }

    pub fn set_prefix(&mut self, prefix: &str) {
        self.prefix = prefix.to_string();
    }

    // Special columns
    pub fn set_id(&mut self, col: &str) -> Result<(), Box<dyn Error>> {
        self.id = Some(self.get_col_index(col)?);
        Ok(())
    }

    pub fn set_addr1(&mut self, col: &str) -> Result<(), Box<dyn Error>> {
        self.addr1 = Some(self.get_col_index(col)?);
        Ok(())
    }

    pub fn set_addr2(&mut self, col: &str) -> Result<(), Box<dyn Error>> {
        self.addr2 = Some(self.get_col_index(col)?);
        Ok(())
    }

    pub fn set_city(&mut self, col: &str) -> Result<(), Box<dyn Error>> {
        self.city = Some(self.get_col_index(col)?);
        Ok(())
    }

    pub fn set_state(&mut self, col: &str) -> Result<(), Box<dyn Error>> {
        self.state = Some(self.get_col_index(col)?);
        Ok(())
    }

    pub fn set_zipcode(&mut self, col: &str) -> Result<(), Box<dyn Error>> {
        self.zipcode = Some(self.get_col_index(col)?);
        Ok(())
    }

    pub fn set_lat(&mut self, col: &str)  -> Result<(), Box<dyn Error>> {
        let index = self.get_col_index(col)?;

        let mut column = self.data.remove(index);
        self.lat = Some(column.iter_mut().map(|e| e.parse::<f64>().unwrap()).collect());
        self.headers.remove(index);

        Ok(())
    }

    pub fn set_lng(&mut self, col: &str) -> Result<(), Box<dyn Error>> {
        let index = self.get_col_index(col)?;

        let mut column = self.data.remove(index);
        self.lng = Some(column.iter_mut().map(|e| e.parse::<f64>().unwrap()).collect());
        self.headers.remove(index);

        Ok(())
    }

    pub async fn fetch(&mut self, key: String) -> Result<(), Box<dyn Error>> {
        println!("Fetching {} coords for {}:", self.shape.1, self.path);

        // collect addresses into a vec
        let mut addresses = Vec::with_capacity(self.shape.1);
        for row in 0..self.shape.1 {
            addresses.push(self.get_address(row));
        }

        // Google's geocoding api will block us if we exceed 50 requests per second
        let requests_per_second: usize = 30;
        let dur = Duration::from_secs_f64(1.0/(requests_per_second as f64));
        let mut clock = tokio::time::interval(dur);

        // Semaphore to make sure we don't max out open http connections
        let sem = Arc::new(Semaphore::new(30));

        // Collection of async tasks which we will join on
        let mut tasks = Vec::with_capacity(self.shape.1);

        // Progress bar to track fetching  progress
        let bar = Arc::new(Mutex::new(ProgressBar::new(self.shape.1 as u64)));

        // Shared client for http requests
        let client = Arc::new(Client::new());

        for row in 0..self.shape.1 {
            let bar_clone = bar.clone();
            let client_clone = client.clone();
            let addr = self.get_address(row);
            let key_clone = key.clone();
            let sem_clone = sem.clone();

            // Rate limit
            clock.tick().await;

            tasks.push(tokio::spawn(async move {
                if addr.is_none() {
                    bar_clone.lock().unwrap().inc(1);
                    return (f64::NAN, f64::NAN, "".to_string());
                }
                let _permit = sem_clone.acquire().await.unwrap();
                let res = fetch_single(&client_clone, addr.unwrap().as_str(), key_clone.as_str()).await.unwrap();
                bar_clone.lock().unwrap().inc(1);
                res
            }));
        }

        let results = join_all(tasks).await;
        bar.lock().unwrap().finish();

        // Add lat and lng rows
        self.lat = Some(Vec::with_capacity(self.shape.1));
        self.lng = Some(Vec::with_capacity(self.shape.1));

        // Add row for normalized address
        self.headers.push("norm_address".to_string());
        self.data.push(Vec::with_capacity(self.shape.1));
        let addr_row = self.data.last_mut().unwrap();

        for result in results {
            let (lat, lng, addr) = result.unwrap();
            self.lat.as_mut().unwrap().push(lat);
            self.lng.as_mut().unwrap().push(lng);
            addr_row.push(addr);
        }

        // Output File

        let path = Path::new(self.path.as_str());
        let path = format!("{}_coords.csv", path.file_stem().unwrap().to_str().unwrap());

        println!("Writing output to {}.", path);
        let mut writer = WriterBuilder::new()
            .delimiter(self.delimiter as u8)
            .from_path(path)?;

        // Print Headers
        let mut new_headers = StringRecord::new();

        for header in self.headers.iter() {
            new_headers.push_field(header);
        }

        new_headers.push_field("lat");
        new_headers.push_field("lng");
        writer.write_record(&new_headers)?;

        let width = self.data.len();
        let height = self.data[0].len();

        // Print data with lat, lng pairs
        for row in 0..height {
            let mut record = StringRecord::new();
            for col in 0..width {
                record.push_field(self.data[col][row].as_str());
            }
            record.push_field(self.lat.as_ref().unwrap()[row].to_string().as_str());
            record.push_field(self.lng.as_ref().unwrap()[row].to_string().as_str());

            writer.write_record(&record)?;
        }

        writer.flush()?;

        Ok(())
    }

    fn get_address(&self, row: usize) -> Option<String> {
        let addr1 = self.data[self.addr1.unwrap()][row].as_str();
        let city = self.data[self.city.unwrap()][row].as_str();
        let state = self.data[self.state.unwrap()][row].as_str();

        let mut parts = vec![addr1, city, state];
        if parts.iter().map(|e| e.trim()).any(|e| e.is_empty()) {
            return None;
        }

        if let Some(zipcode) = self.zipcode {
            let zipcode = self.data[zipcode][row].as_str();
            parts.push(zipcode);
        }

        if let Some(addr2) = self.addr2 {
            let addr2 = self.data[addr2][row].as_str();
            parts.insert(1, addr2);
        }

        Some(parts.join(" "))
    }

    pub fn output_headers(&self) -> Vec<String> {
        let mut headers = Vec::new();
        for col in self.output_cols.iter() {
            if self.prefix.is_empty() {
                headers.push(self.headers[*col].clone())
            } else {
                headers.push(format!("{}_{}", self.prefix, self.headers[*col].clone()));
            }
        }

        headers
    }

    pub fn output_row(&self, row: usize) -> Vec<String> {
        let mut output_row = Vec::new();
        for col in self.output_cols.iter() {
            output_row.push(self.data[*col][row].clone());
        }

        output_row
    }

    pub fn compare_row(&self, row: usize) -> Vec<String> {
        let mut compare_row = Vec::new();
        for col in self.compare_cols.iter() {
            compare_row.push(self.data[*col][row].clone());
        }

        compare_row
    }

    pub fn remove_row(&mut self, row: usize) {
        if let Some(lat) = &mut self.lat {
            lat.remove(row);
        }

        if let Some(lng) = &mut self.lng {
            lng.remove(row);
        }

        for col in self.data.iter_mut() {
            col.remove(row);
        }

        self.shape.1 -= 1;
    }
}

async fn fetch_single(client: &Client, addr: &str, key: &str) -> Result<(f64, f64, String), Box<dyn Error>> {
    let params = [("address", addr), ("key", key)];
    let res = client.get("https://maps.googleapis.com/maps/api/geocode/json")
        .query(&params)
        .send()
        .await?;

    if !res.status().is_success() {
        println!("error fetching {}", addr);
    }

    let text = res.text().await?;

    let json: Value = serde_json::from_str(text.as_str()).unwrap();
    let lat = json["results"][0]["geometry"]["location"]["lat"].as_f64();
    let lng = json["results"][0]["geometry"]["location"]["lng"].as_f64();
    let addr = json["results"][0]["formatted_address"].as_str();

    if lat.is_some() || lng.is_some() {
        let lat = lat.unwrap();
        let lng = lng.unwrap();
        let addr = addr.unwrap_or("").to_string();

        Ok((lat, lng, addr))
    } else {
        println!("{}", json);
        if let Some(status) = json["status"].as_str() {
            if status=="OVER_QUERY_LIMIT" {
                println!("\nMaxed Out API KEY\n");
            }
        }
        Ok((f64::NAN, f64::NAN, "".to_string()))
    }
}
