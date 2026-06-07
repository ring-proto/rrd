use std::io::{self, Read, Write};
use std::process::ExitCode;

use rrd_cni::{cni_error_json, run_cni, CniInvocation};

fn main() -> ExitCode {
    let mut stdin = String::new();
    if let Err(error) = io::stdin().read_to_string(&mut stdin) {
        let _ = writeln!(io::stdout(), "{{\"cniVersion\":\"1.1.0\",\"code\":\"100\",\"msg\":\"failed to read stdin: {error}\"}}");
        return ExitCode::FAILURE;
    }

    match run_cni(CniInvocation::from_env(stdin)) {
        Ok(output) => {
            if !output.stdout.is_empty() {
                let _ = io::stdout().write_all(output.stdout.as_bytes());
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            let _ = io::stdout().write_all(cni_error_json(&error).as_bytes());
            ExitCode::FAILURE
        }
    }
}
