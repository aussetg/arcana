fn main() {
    if let Err(error) = arcana::cli::run() {
        if error
            .downcast_ref::<arcana::output::error::AlreadyReportedError>()
            .is_some()
        {
            std::process::exit(1);
        }
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}
