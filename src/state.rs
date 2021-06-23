use std::error::Error;
use csv::{StringRecord, WriterBuilder};
use indicatif::ProgressBar;

use super::data_frame::DataFrame;

const R: f64 = 3958.8; // Radius of Earth (miles)

#[derive(PartialEq)]
enum MatchMode {
    LEFT,   // match onto leftmost file, thus only entries in the left file appear
    INNER   // only print entries that match, from any file
    // eventually outer when I feel like it
}

// Config object holds configs for each file, where each index acts as that
// files "id"
pub struct State {
    data_frames: Vec<DataFrame>,
    file_count: usize,
    match_mode: MatchMode,
    api_key: String
}


impl State {
    pub fn new(api_key: String) -> State {
        State {
            data_frames: Vec::new(),
            file_count: 0,
            match_mode: MatchMode::LEFT,
            api_key
        }
    }

    pub fn print(&self) {
        for (i, df) in self.data_frames.iter().enumerate() {
            println!("{}: {}", i, df);
        }
    }

    // Check if the state is ready to fetch
    pub fn ready_to_fetch(&self) -> bool {
        for df in self.data_frames.iter() {
            if !df.ready_to_fetch() {return false;}
        }

        true
    }

    // Check if the state is ready to match
    pub fn ready_to_match(&self) -> bool {
        for df in self.data_frames.iter() {
            if !df.ready_to_match() {return false};
        }

        true
    }

    // Add the file name and set all column indexes to None
    // Then try to guess which columns are which indexes, but not to loosely
    pub fn add_file(&mut self, file_name: &str) {
        self.file_count+=1;
        self.data_frames.push(DataFrame::from_path(file_name));
    }

    // Get reader using current config for file
    pub fn get_dataframe(&self, index: usize) -> &DataFrame {
        &self.data_frames[index]
    }

    pub fn set_method<'a>(&mut self, input: Vec<&'a str>) -> Result<(), Box<dyn Error>> {
        let method = input.get(1);
        if method.is_none() {
            return Err("method required")?;
        }

        match *method.unwrap() {
            "left" => {
                self.match_mode = MatchMode::LEFT;
            }
            "inner" => {
                self.match_mode = MatchMode::INNER;
            }
            _ => {
                return Err("Invalid match mode")?;
            }
        }

        Ok(())
    }

    // Add column to output, will be prefixed with prefixes
    pub fn add_match_column<'a>(&mut self, input: Vec<&'a str>) -> Result<(), Box<dyn Error>> {
        let file_index = input.get(1);
        if file_index.is_none() {
            return Err("file_index required")?;
        }
        let file_index = file_index.unwrap().parse::<usize>()?;

        let col_type = input.get(2);
        if col_type.is_none() {
            return Err("type required")?;
        }
        let col_type = col_type.unwrap();

        if input.len() < 4 {
            return Err("output_col required")?;
        }

        let output_col = input[3..].join(" ");

        if file_index >= self.file_count {
            return Err("Index out of Bounds")?;
        }

        if col_type.eq(&"output") {
            self.data_frames[file_index].add_output_column(output_col.as_str());
        } else if col_type.eq(&"compare") {
            self.data_frames[file_index].add_compare_column(output_col.as_str());
        } else {
            return Err("Invalid type")?;
        }

        Ok(())
    }

    // Add a prefix for all columns from a certain file
    pub fn set_prefix<'a>(&mut self, input: Vec<&'a str>) -> Result<(), Box<dyn Error>> {

        let file_index = input.get(1);
        if file_index.is_none() {
            return Err("file_index required")?;
        }
        let file_index = file_index.unwrap().parse::<usize>()?;

        let prefix = input.get(2);
        if prefix.is_none() {
            return Err("prefix required")?;
        }
        let prefix = prefix.unwrap();

        if file_index >= self.file_count {
            return Err("Index out of Bounds")?;
        }

        self.data_frames[file_index].set_prefix(prefix);

        Ok(())
    }

    pub fn get_columns<'a>(&mut self, input: Vec<&'a str>) -> Result<&Vec<String>, Box<dyn Error>> {
        // Check for file_index
        let file_index = input.get(1);
        if file_index.is_none() {
            return Err("file_index required")?;
        }

        // Check that file_index is valid and retreive its name
        let file_index = file_index.unwrap().parse::<usize>()?;
        if file_index >= self.file_count {
            return Err("Index out of Bounds")?;
        }

        Ok(self.data_frames[file_index].get_headers())
    }

    pub fn set_param<'a>(&mut self, input: Vec<&'a str>) -> Result<(), Box<dyn Error>> {
        let index = input.get(1);
        let key = input.get(2);

        if index.is_none() {
            return Err("index is required")?;
        }else if key.is_none() {
            return Err("key is required")?;
        } else if input.len() < 3 {
            return Err("val is required")?;
        }

        let val = &input[3..].join(" ");
        let index = index.unwrap().parse::<usize>()?;
        let key = key.unwrap();

        if index >= self.file_count {
            return Err("Index out of Bounds")?;
        }

        let df = &mut self.data_frames[index];

        match key.to_lowercase().as_str() {
            "addr1" => df.set_addr1(val),
            "addr2" => df.set_addr2(val),
            "city" => df.set_city(val),
            "state" => df.set_state(val),
            "zipcode" => df.set_zipcode(val),
            "lat" => df.set_lat(val),
            "lng" => df.set_lng(val),
            _ => {}
        }

        Ok(())
    }

    pub async fn fetch(&mut self) -> Result<(), Box<dyn Error>> {
        for df in self.data_frames.iter_mut() {
            df.fetch(self.api_key.clone()).await?;
        }

        Ok(())
    }

    pub fn find_matches(&mut self) -> Result<(), Box<dyn Error>> {
        let df1 = self.data_frames.get(0).unwrap();
        let df2 = self.data_frames.get(1).unwrap();

        let bar = ProgressBar::new(df1.shape.1 as u64);
        let mut writer = WriterBuilder::new()
            .delimiter('|' as u8)
            .from_path("matches.csv")?;

        // Get new headers
        let mut headers = df1.output_headers();
        headers.append(&mut df2.output_headers());
        headers.push("distance".to_string());

        writer.write_record(headers.as_slice())?;
        let mut num_matches = 0;

        // For each row in the left dataset, compare to every row in the right dataset
        // if latitude and longitude are equal it is exact match,
        // else find the minimum linear distance and then convert to haversine distance
        for row in 0..df1.shape.1 {
            let lat = df1.lat().unwrap()[row];
            let lng = df1.lng().unwrap()[row];
            let mut min: Option<(usize, f64)> = None;

            if !(lat.is_nan() || lng.is_nan()) {

                for test_row in 0..df2.shape.1 {
                    let test_lat = df2.lat().unwrap()[test_row];
                    let test_lng = df2.lng().unwrap()[test_row];

                    if test_lat.is_nan() || test_lng.is_nan() {
                        continue;
                    }

                    if lat==test_lat && lng==test_lng {
                        min = Some((test_row, 0.));
                        break;
                    }

                    let dist = linear(lat, lng, test_lat, test_lng);
                    if min.is_none() || dist < min.unwrap().1 {
                        min = Some((test_row, dist));
                    }
                }

                // Correct distance if necessary
                if let Some((min_row, dist)) = min {
                    if dist != 0. {
                        let min_lat = df2.lat().unwrap()[min_row];
                        let min_lng = df2.lng().unwrap()[min_row];
                        min.unwrap().1 = haversine(lat, lng, min_lat, min_lng);
                    }
                }
            }

            match &self.match_mode {
                MatchMode::LEFT => {

                }
                MatchMode::INNER => {}
            }

            if min.is_some() || self.match_mode==MatchMode::LEFT {
                // Write record
                let mut new_record = StringRecord::new();

                for col in df1.output_row(row) {
                    new_record.push_field(col.as_str());
                }

                if let Some((min_row, dist)) = min {
                    num_matches+=1;
                    for col in df2.output_row(min_row) {
                        new_record.push_field(col.as_str());
                    }
                    new_record.push_field(dist.to_string().as_str());
                } else {
                    for _ in df2.output_headers() {
                        new_record.push_field("");
                    }
                    new_record.push_field("");
                }

                writer.write_record(&new_record)?;
            }

            bar.inc(1);
        }

        bar.finish();
        println!("Found {} matches", num_matches);

        Ok(())
    }
}

fn linear(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    ((lat2 - lat1).powi(2) + (lng2 - lng1).powi(2)).sqrt()
}

fn haversine(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let delta_lat = (lat2-lat1).to_radians();
    let delta_lng = (lng2-lng1).to_radians();

    let a = (delta_lat*0.5).sin().powi(2) + lat1.to_radians().cos() * lat2.to_radians().cos() * (delta_lng*0.5).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0-a).sqrt());
    R * c
}
