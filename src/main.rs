use std::io::{self, Write};
use std::{array, mem, process};
use std::fmt;
use std::convert::TryInto;

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE:usize = 255;
const PAGE_SIZE:usize = 4096;
const TABLE_MAX_PAGES: usize = 100;

const ID_SIZE: usize = mem::size_of::<u32>();
const USERNAME_SIZE: usize = COLUMN_USERNAME_SIZE;
const EMAIL_SIZE: usize = COLUMN_EMAIL_SIZE;
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;


struct Row {
 id: u32,
 username: [u8; COLUMN_USERNAME_SIZE], // Fixed-size array for username
 email: [u8; COLUMN_EMAIL_SIZE],   // Fixed-size array for email
}

impl Row {
    fn new(id: u32, username: &str, email: &str) -> Self{
        let mut row = Row {
            id,
            username: [0;COLUMN_USERNAME_SIZE],
            email: [0;COLUMN_EMAIL_SIZE]
        };
        row.username[..username.len()].copy_from_slice(username.as_bytes());
        row.email[..email.len()].copy_from_slice(email.as_bytes());
        return row;
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Convert the byte arrays to strings, trimming null bytes
        let username = std::str::from_utf8(&self.username)
            .unwrap()
            .trim_end_matches('\0');
        let email = std::str::from_utf8(&self.email)
            .unwrap()
            .trim_end_matches('\0');
        // Write the formatted output
        write!(f, "({}, {}, {})", self.id, username, email)
    }
}

struct Table {
    num_rows: usize,
    pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES]
}

impl Table {
    fn new() -> Self{
        Table{
            num_rows: 0,
            pages: array::from_fn(|_| None)
        }
    }
    fn row_slot(&mut self, row_num: usize) -> &mut[u8]{
        // page number
        // fetch the page number from self.pages
        // if not available create a new one with Box::new([0, PAGE_SIZE])
        // find byte_offset = (row_num % TOTAL_ROWS_PER_PAGE) * ROW_SIZE
        // return self.page[byte_offset..byte_offset+ROW_SIZE]
        let page_number = row_num / ROWS_PER_PAGE;
        let page =  self.pages[page_number].get_or_insert_with(|| Box::new([0; PAGE_SIZE]));
        let byte_offset  = (row_num % ROWS_PER_PAGE) * ROW_SIZE;
        return &mut page[byte_offset..byte_offset+ROW_SIZE];
    }
}

enum MetaCommentResults{
    MetaCommandSuccess,
    MetaCommandUnknown
}
enum PrepareStatement {
    PrepareSuccess,
    PrepareSyntaxError,
    PrepareUnrecognizedCommand
}

enum StatementType {
    Insert,
    Select,
}
struct Statement {
    _type: StatementType,
    row_to_insert: Row, // Only used by insert statement
}

struct InputBuffer {
    buffer:String,
    buffer_length:usize,
    input_length:usize
}

enum ExecuteResult {
    Success,
    TableFull,
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

fn prepare_statement(input_buffer: &InputBuffer, statement:&mut Statement) -> PrepareStatement{
    // I need to parse the input buffer and store the id, username and email in the statement.row_to_insert variable

    if input_buffer.buffer.starts_with("insert"){
        let mut parts = input_buffer.buffer.split_whitespace();
        if parts.clone().count() < 4{
            return PrepareStatement::PrepareSyntaxError;
        }
        
        // Add error handling for id parsing
        let id_str = parts.nth(1).unwrap();
        let id = match id_str.parse::<u32>() {
            Ok(val) => val,
            Err(_) => return PrepareStatement::PrepareSyntaxError,
        };
        
        let username = parts.next().unwrap();
        let email = parts.next().unwrap();
        statement._type = StatementType::Insert;
        statement.row_to_insert = Row::new(id, username, email);
        return PrepareStatement::PrepareSuccess;
    }
    if input_buffer.buffer == "select"{
        statement._type = StatementType::Select;
        return PrepareStatement::PrepareSuccess
    }
    return PrepareStatement::PrepareUnrecognizedCommand;
}


fn serialize_row(row: &Row, destination: &mut [u8]) {
    destination[ID_OFFSET..ID_OFFSET + ID_SIZE].copy_from_slice(&row.id.to_ne_bytes());
    destination[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE].copy_from_slice(&row.username);
    destination[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE].copy_from_slice(&row.email);
}

fn deserialize_row(source: &[u8], destination: &mut Row) {
    destination.id = u32::from_ne_bytes(source[ID_OFFSET..ID_OFFSET + ID_SIZE].try_into().unwrap());
    destination.username.copy_from_slice(&source[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE]);
    destination.email.copy_from_slice(&source[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE]);
}

fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult{
    if table.num_rows > TABLE_MAX_ROWS{
        return ExecuteResult::TableFull;
    }
    serialize_row(&statement.row_to_insert, table.row_slot(table.num_rows));
    table.num_rows += 1;
    return ExecuteResult::Success
}

fn execute_select(table: &mut Table) -> ExecuteResult{
    let mut row = Row::new(0, "", "");
    for i in 0..table.num_rows{
        deserialize_row(&table.row_slot(i), &mut row);
        println!("{}", row);
    }
    return ExecuteResult::Success;
}

fn execute_statement(statement: &Statement, table: &mut Table) -> ExecuteResult{
    match statement._type {
        StatementType::Insert => { execute_insert(statement, table)}
        StatementType::Select => { execute_select(table)}
    }
}


fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn main() {
    let mut input_buffer: InputBuffer;
    let mut table = Table::new();
    input_buffer = InputBuffer::new();

    loop {
        print_prompt();
        input_buffer.read_input();

        let mut statement = Statement {
            _type: StatementType::Select,
            row_to_insert: Row::new(0, "", ""),
        };

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

        match prepare_statement(&input_buffer, &mut statement) {
            PrepareStatement::PrepareSuccess => {
            }
            PrepareStatement::PrepareUnrecognizedCommand =>{
                println!("Unrecognized command at the start of {}", input_buffer.buffer);
                continue;
            }
            PrepareStatement::PrepareSyntaxError =>{
                println!("SyntaxError at the start of {}", input_buffer.buffer);
                continue;
            }
        }
        match execute_statement(&statement, &mut table) {
            ExecuteResult::Success => println!("Executed."),
            ExecuteResult::TableFull => println!("Error: Table full."),
        }
    }
}