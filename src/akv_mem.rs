use actionkv_db::ActionKV;


#[cfg(target_os = "windows")]
const TEST_USAGE: &str = "
Usage:
    akv_mem.exe FILE get KEY
    akv_mem.exe FILE delete KEY
    akv_mem.exe FILE insert KEY VALUE
    akv_mem.exe FILE update KEY VALUE
";

#[cfg(not(target_os = "windows"))]
const TEST_USAGE: &str = "
Usage:
    akv_mem FILE get KEY
    akv_mem FILE delete KEY
    akv_mem FILE insert KEY VALUE
    akv_mem FILE update KEY VALUE
";

fn main() -> (){
    let args: Vec<String> = std::env::args().collect();
    let fname= args.get(1).expect(&TEST_USAGE);
    let action: &str = args.get(2).expect(&TEST_USAGE).as_ref();
    let key: &str= args.get(3).expect(&TEST_USAGE).as_ref();
    let possible_value = args.get(4);

    let path = std::path::Path::new(&fname);
    let mut store = ActionKV::open(path).expect("Unable to open file");
    store.load().expect("Unable to Load data");

    match action{
        "get" => match store.get(key.as_bytes()).unwrap(){
            None => eprintln!("{:?} not found!", key),
            Some(var) => println!("{}", String::from_utf8(var).unwrap())
        },

        "delete" => store.delete(key.as_bytes()).unwrap(),
        "insert" => store.insert(key.as_bytes(), possible_value.unwrap().as_bytes().as_ref()).unwrap(),
        "update" => store.update(key.as_bytes(), possible_value.unwrap().as_bytes().as_ref()).unwrap(),
        _ => eprintln!("{}", &TEST_USAGE)
    }

    
}