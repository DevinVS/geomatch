use clap::{Arg, App};
use std::io::{stdin, stdout, Write};
use geomatch::state::State;

#[tokio::main]
async fn main() -> Result<(),()> {
    // Get cli options
    let matches = App::new("GeoMatch")
        .version("1.0")
        .author("Devin Vander Stelt <devin@vstelt.dev>")
        .about("Utility for fetching and matching csv files")
        .arg(Arg::with_name("files").required(true).min_values(1))
        .arg(Arg::with_name("api-key").short("k").takes_value(true).required(true).env("API_KEY"))
        .get_matches();

    let mut cli_state = State::new(matches.value_of("api-key").unwrap().to_string());

    // Load config and try to guess good defaults
    for file_name in matches.values_of("files").unwrap() {
        cli_state.add_file(file_name);
    }

    // Init cli interface
    print_splash();
    print_prompt();

    // Keep processing commands until user quits
    let mut input_buffer = String::with_capacity(20);
    while let Ok(_) = stdin().read_line(&mut input_buffer) {
        let input: Vec<&str> = input_buffer.trim().split_whitespace().to_owned().collect();
        let cmd = input.get(0);

        if cmd.is_none() {
            input_buffer.clear();
            print_prompt();
            continue;
        }
        let cmd = cmd.unwrap();

        let result = match *cmd {
            "list" => {
                let columns = cli_state.get_columns(input);
                match columns {
                    Ok(columns) => {
                        for col in columns {
                            println!("\t{}", col);
                        }
                        Ok(())
                    }
                    Err(e) => {
                        Err(e)
                    }
                }
            },
            "config" => {
                cli_state.print();
                Ok(())
            },
            "set" => {
                cli_state.set_param(input)
            },
            "fetch" => {
                if cli_state.ready_to_fetch() {
                    cli_state.fetch().await
                } else {
                    Err("Invalid config for fetch".into())
                }
            },
            "match" => {
                if cli_state.ready_to_match() {
                    cli_state.find_matches()
                } else {
                    Err("Invalid config for match".into())
                }
            },
            "add" => {
                cli_state.add_match_column(input)
            }
            "method" => {
                cli_state.set_method(input)
            }
            "radius" => {
                cli_state.set_radius(input)
            },
            "exclusive" => {
                cli_state.set_exclusive(input)
            }
            "quit" => {
                break;
            },
            "help" => {
                print_help();
                Ok(())
            },
            "prefix" => {
                cli_state.set_prefix(input)
            },
            _ => {
                println!("Unknown command: '{}'", cmd);
                print_help();
                Ok(())
            }
        };

        // Print error, if any
        match result {
            Err(e) => println!("{}", e),
            Ok(_) => {}
        }

        input_buffer.clear();
        print_prompt();
    }

    Ok(())
}


fn print_help() {
    const HELP_MSG: &str = {
        r#"HELP:
    list [index]        List out all columns in the file with index
    set [index] [var] [col]     Assign a column to a runtime variable
        fetch var Options:
            addr1   [required]
            addr2   [optional]
            city    [required]
            state   [required]
            zipcode [required]
        match var Options:
            lat     [required]
            lng     [required]
    add [index] [type] [col]       Add a column for a specific purpose
        type Options:
            output      Write the column to the csv file
            compare     Use the column to differentiate between duplicate locations
    prefix [index] [val]    Set prefix for a specified file's columns
    method [method]     Set method for matching
        method Options:
            left    Include all entries from the first file its matches
            inner   Include all entries that had a positive match
    radius [radius]     Defaults to 0.25 miles. Max radius for two locations to be considered a match.
    exclusive [true or false]   Defaults to true. Determines whether an entry can match to more than
        one entry. Non-Exclusive makes the most sense when combined with a left join, effectively giving
        the closest match per each location.
    config  Print out the current configuration
    fetch   Fetch all the coordinate pairs and write to new csv file
    match   Match all the files together and write to new csv file
    quit    Quit the application
    help    List out this help message
        "#
    };
    println!("{}", HELP_MSG);
}

fn print_splash() {
    const SPLASH: &str = {
r#"---------------------- GEOMATCH -------------------------
type help to see commands and options
"#
    };

    println!("{}", SPLASH);
}

fn print_prompt() {
    print!("geomatch> ");
    stdout().flush().unwrap();
}

