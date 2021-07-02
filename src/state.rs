use std::error::Error;
use csv::WriterBuilder;
use indicatif::ProgressBar;
use fuzzywuzzy::fuzz::token_sort_ratio;
use super::data_frame::DataFrame;

const R: f64 = 3958.8; // Radius of Earth (miles)

#[derive(PartialEq, Debug)]
enum MatchMode {
    LEFT,   // match onto leftmost file, thus only entries in the left file appear
    INNER,  // only print entries that match, from any file
    OUTER,  // Print all unique entries
}

// Config object holds configs for each file, where each index acts as that
// files "id"
pub struct State {
    data_frames: Vec<DataFrame>,
    file_count: usize,
    match_mode: MatchMode,
    api_key: String,
    radius: f64,
    exclusive: bool
}


impl State {
    pub fn new(api_key: String) -> State {
        State {
            data_frames: Vec::new(),
            file_count: 0,
            match_mode: MatchMode::LEFT,
            api_key,
            radius: 0.25,
            exclusive: true
        }
    }

    pub fn print(&self) {
        for (i, df) in self.data_frames.iter().enumerate() {
            println!("{}: {}", i, df);
        }
        println!("Radius: {}", self.radius);
        println!("MatchMode: {:?}", self.match_mode);
        println!("Exclusive: {}", self.exclusive);
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
            "outer" => {
                self.match_mode = MatchMode::OUTER;
            }
            _ => {
                return Err("Invalid match mode")?;
            }
        }

        Ok(())
    }

    pub fn set_exclusive<'a>(&mut self, input: Vec<&'a str>) -> Result<(), Box<dyn Error>> {
        let val = input.get(1);
        if val.is_none() {
            return Err("val required")?;
        }
        let val = val.unwrap();

        match val.to_lowercase().as_str() {
            "true" => {
                self.exclusive = true;
            },
            "false" => {
                self.exclusive = false;
            }
            _ => {
                return Err("val must be true or false")?;
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
            self.data_frames[file_index].add_output_column(output_col.as_str())?;
        } else if col_type.eq(&"compare") {
            self.data_frames[file_index].add_compare_column(output_col.as_str())?;
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

    // Set matching radius
    pub fn set_radius<'a>(&mut self, input: Vec<&'a str>) -> Result<(), Box<dyn Error>> {
        let radius = input.get(1);
        if radius.is_none() {
            return Err("radius required")?;
        }
        self.radius = radius.unwrap().parse::<f64>()?;

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
            "addr1" => df.set_addr1(val)?,
            "addr2" => df.set_addr2(val)?,
            "city" => df.set_city(val)?,
            "state" => df.set_state(val)?,
            "zipcode" => df.set_zipcode(val)?,
            "lat" => df.set_lat(val)?,
            "lng" => df.set_lng(val)?,
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
        let (width, height) = {
            let mut width = 0;
            let mut height = 0;

            for df in self.data_frames.iter() {
                width += df.output_headers().len();
                height += df.shape.1;
            }
            if self.match_mode==MatchMode::LEFT {
                width+=1;
            }

            (width, height)
        };

        // if width is 0 no output columns were supplied
        if width == 0 {
            return Err("No output columns supplied")?;
        }

        let bar = ProgressBar::new(height as u64);

        // create output dataframe, technically overprovisioned for the height
        let mut output = DataFrame::with_capacity(width, height);

        // Keep track of which columns inside output contain a match
        let mut match_mask: Vec<bool> = Vec::with_capacity(height);
        for _ in 0..height {
            match_mask.push(false);
        }

        // Set the headers
        let mut headers = Vec::with_capacity(width);
        for df in self.data_frames.iter() {
            for header in df.output_headers() {
                headers.push(header.clone());
            }
        }

        if self.match_mode==MatchMode::LEFT {
            headers.push("distance".to_string());
        }

        output.set_headers(headers);

        // Make sure every column is an output column
        for i in 0..width {
            output.output_cols.push(i);
        }

        // We start by assuming that each file is internally consistent, meaning
        // that if a location is duplicated inside it that is by design as they
        // represent two separate entities.
        // Now we begin the matching process. For each of the remaining dataframes,
        // we need to find the nearest match, add it to its respective rows, and then
        // average the latitude and longitude. If no match is found, we create a new row
        // for the entry. On the first run no matches will be found so the dataframe will be
        // essentially copied into the output
        let mut col_index = 0;

        for df_index in 0..self.data_frames.len() {
            // Clone dataframe so we can subtract from it as we match
            let df = &self.data_frames[df_index];
            let mut written_mask = Vec::with_capacity(df.shape.1);
            for _ in 0..df.shape.1 {
                written_mask.push(false);
            }
            let cols = df.output_headers().len();

            // This part is a little bizarre, we are going to iterate throught the existing entries
            // in the output dataframe. This keeps us from overwriting our matches and allows for a
            // more uniform process for each dataframe
            for row in 0..output.data()[0].len() {
                let result = self.find_single_match(row, &output, &df, &written_mask);

                if let Some((index, dist)) = result {
                    // Add to output
                    let output_cols = df.output_row(index);
                    for col in 0..cols {
                        output.data_mut()[col_index+col][row] = output_cols[col].clone();
                    }

                    if self.match_mode==MatchMode::LEFT {
                        output.data_mut().last_mut().unwrap()[row] = dist.to_string();
                    }

                    // Average coordinates
                    let lat = (output.lat().unwrap()[row] + df.lat().unwrap()[index]) * 0.5;
                    let lng = (output.lng().unwrap()[row] + df.lng().unwrap()[index]) * 0.5;

                    output.lat_mut().unwrap()[row] = lat;
                    output.lng_mut().unwrap()[row] = lng;

                    // Set mask to not include for writing at the end
                    written_mask[index] = true;

                    // Set match_mask
                    match_mask[row] = true;

                    bar.inc(1);
                }
            }

            // Now that we've fitered out all the matches, we can just append all the rest of the
            // rows. On a left join we only do this if the dataframe index is 0
            if self.match_mode!=MatchMode::LEFT || df_index==0 {
                for row in 0..self.data_frames[df_index].shape.1 {
                    if !self.exclusive || !written_mask[row] {
                        // Fill previous slots with blanks
                        for col in 0..col_index {
                            output.data_mut()[col].push("".to_string());
                        }

                        // Fill in the actual data
                        let output_cols = df.output_row(row);
                        for col in 0..cols {
                            output.data_mut()[col+col_index].push(output_cols[col].clone());
                        }
                        output.lat_mut().unwrap().push(df.lat().unwrap()[row]);
                        output.lng_mut().unwrap().push(df.lng().unwrap()[row]);

                        // Fill rest of slots with blanks
                        for col in col_index+cols..width {
                            output.data_mut()[col].push("".to_string());
                        }

                        bar.inc(1);
                    }
                }
            } else {
                bar.inc(written_mask.iter().filter(|e| !*e).count() as u64)
            }

            col_index += cols;
        }

        bar.finish();

        // At this point we theoretically have a complete dataset, lets write it to the filesystem
        // and be done

        let mut writer = WriterBuilder::new()
            .delimiter('|' as u8)
            .from_path("matches.csv")?;

        writer.write_record(output.output_headers().as_slice())?;

        // If match_mode is left, we only have items from the leftmost table already so no checks are
        // required. If inner, we can use our match_mask to make sure only columns with existing matches exist
        // Outer we just write everything as is
        for row in 0..output.data()[0].len() {
            if self.match_mode!=MatchMode::INNER || match_mask[row] {
                writer.write_record(output.output_row(row).as_slice())?;
            }
        }

        Ok(())
    }

    fn find_single_match(&self, record_index: usize, df1: &DataFrame, df2: &DataFrame, written_mask: &Vec<bool>) -> Option<(usize, f64)> {
        let lat = df1.lat().unwrap()[record_index];
        let lng = df1.lng().unwrap()[record_index];

        if lat.is_nan() || lng.is_nan() {
            return None;
        }

        let mut exact: Vec<usize> = Vec::new();
        let mut min: Option<(usize, f64, f64, f64)> = None;

        for test_index in 0..df2.shape.1 {
            if self.exclusive && written_mask[test_index] {
                continue;
            }

            let test_lat = df2.lat().unwrap()[test_index];
            let test_lng = df2.lng().unwrap()[test_index];

            if test_lat.is_nan() || test_lng.is_nan() {
                continue;
            }

            if lat==test_lat && lng==test_lng {
                exact.push(test_index);
                continue;
            } else if exact.len() != 0 {
                continue;
            }

            let dist = linear(lat, lng, test_lat, test_lng);
            if min.is_none() || dist < min.unwrap().3 {
                min = Some((test_index, test_lat, test_lng, dist));
            }
        }

        // If we have a single exact match just return it
        if exact.len() == 1 {
            return Some((exact[0], 0.));
        }

        // If we have multiple exact matches we have to guess with compare
        // columns which one suits it best
        if exact.len() > 1 {
            let src_compare = df1.compare_row(record_index);

            // The basic idea here is to find the row that has the minimum squared 
            // distance from the compare row
            let mut min: Option<(usize, usize)> = None;
            for test_index in exact {
                let test_compare = df2.compare_row(test_index);
                let mut dist = 0;

                // For each column find the closest compare column
                for test_col in test_compare.iter() {
                    let mut min_col_dist = None;
                    for src_col in src_compare.iter() {
                        let col_dist = 100-token_sort_ratio(&src_col, &test_col, true, true) as usize;
                        if min_col_dist.is_none() || min_col_dist.unwrap() > col_dist {
                            min_col_dist = Some(col_dist);
                        }
                    }
                    if min_col_dist.is_some() {
                        dist += min_col_dist.unwrap().pow(2);
                    }
                }

                if min.is_none() || min.unwrap().1 > dist {
                    min = Some((test_index, dist));
                }
            }

            return Some((min.unwrap().0, 0.0))
        }

        if let Some((min_index, min_lat, min_lng, mut dist)) = min {
            dist = haversine(lat, lng, min_lat, min_lng);
            if dist > self.radius {
                return None;
            }

            return Some((min_index, dist));
        }


        None
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
