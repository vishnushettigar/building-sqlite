use std::io::{self, Write};
use std::process;

// const COLUMN_USERNAME_SIZE: usize = 32;
// const COLUMN_EMAIL_SIZE:usize = 255;

// struct Row {
//  id: u32,
//  username: [u8; COLUMN_USERNAME_SIZE], // Fixed-size array for username
//  email: [u8; COLUMN_EMAIL_SIZE],   // Fixed-size array for email
// }

enum MetaCommentResults{
    MetaCommandSuccess,
    MetaCommandUnknown
}
enum PrepareStatement {
    PrepareSuccess,
    // PrepareSyntaxError,
    PrepareUnrecognizedCommand
}

enum StatementType {
    Insert,
    Select,
    Unknown,
}

struct Statement {
    _type: StatementType,
    // row_to_insert: Row,
}

struct InputBuffer {
    buffer:String,
    buffer_length:usize,
    input_length:usize
}

impl InputBuffer{
    fn new() -> Self{
        InputBuffer{
            buffer: String::new(),
            buffer_length: 0,
            input_length: 0
        }
    }

    fn read_input(&mut self){
        self.buffer.clear();
        match io::stdin().read_line(&mut self.buffer){
            Ok(bytes_read) => {
                if bytes_read <=0 {
                    println!("Error reading input");
                    process::exit(1)
                }
                self.input_length = bytes_read - 1;
                self.buffer.truncate(self.input_length);
            }
            Err(_) => {
                println!("Error reading input");
                process::exit(1)
            }
        }
    }
}



fn do_meta_command(input_buffer: &InputBuffer) -> MetaCommentResults{
    if input_buffer.buffer == ".exit"{
        process::exit(0);
        // return MetaCommentResults::MetaCommandSuccess;
    } else {
        return MetaCommentResults::MetaCommandUnknown;
    }
}

fn prepare_statement(input_buffer: &InputBuffer) -> (PrepareStatement, Statement){
    let statement: Statement;
    if input_buffer.buffer == "insert"{
        statement = Statement{
            _type: StatementType::Insert,
        };
        return (PrepareStatement::PrepareSuccess, statement);
    }
    if input_buffer.buffer == "select"{
        statement = Statement{
            _type: StatementType::Select,
        };
        return (PrepareStatement::PrepareSuccess, statement);
    }
    statement = Statement {
        _type: StatementType::Unknown  // or any default value
    };
    return (PrepareStatement::PrepareUnrecognizedCommand, statement)
}

fn execute_command(statement: Statement) {
    match statement._type {
        StatementType::Insert => {
            println!("Handle insert here.\n");
        }
        StatementType::Select => {
            println!("Handle select here.\n");
        }
        StatementType::Unknown => {
            println!("I don't know how to handle this.\n");
        }
    }
}


fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn main() {
    let mut input_buffer: InputBuffer;
    input_buffer = InputBuffer::new();
    let mut statement: Statement;
    let mut preparation_status: PrepareStatement;
    loop {
        print_prompt();
        input_buffer.read_input();

        if input_buffer.buffer.chars().nth(0) == Some('.') {
            match do_meta_command(&input_buffer) {
                MetaCommentResults::MetaCommandSuccess => {
                    continue;
                }
                MetaCommentResults::MetaCommandUnknown => {
                    println!("Unrecognized command");
                    continue;
                }
            }
        }
        (preparation_status, statement) =  prepare_statement(&input_buffer);

        match preparation_status {
            PrepareStatement::PrepareSuccess => {
            }
            PrepareStatement::PrepareUnrecognizedCommand =>{
                println!("Unrecognized command at the start of {}", input_buffer.buffer);
                continue;
            }
        }
        execute_command(statement);
        println!("Completed Execution");
    }
}