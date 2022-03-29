import sys
import traceback
from contextlib import ExitStack, contextmanager

import dbt.tracking
import dbt.version
from dbt.adapters.factory import cleanup_connections, reset_adapters
from dbt.config import TaskConfig
from dbt.events.functions import fire_event, setup_event_logger
from dbt.events.types import (MainEncounteredError, MainKeyboardInterrupt,
                              MainReportArgs, MainReportVersion,
                              MainStackTrace, MainTrackingUserState)
from dbt.exceptions import (FailedToConnectException, InternalException,
                            NotImplementedException)
from dbt.logger import log_cache_events, log_manager
from dbt.profiler import profiler
from dbt.utils import ExitCodes, args_to_dict
import warnings
import click

@click.group()
def main():
    pass
# docs,source,init,clean,debug,deps,list,ls,build,snapshot,run,compile,parse,test,seed,run-operation
@main.command()
def docs():
    click.echo('Docs go here')

@main.command()
def source():
    click.echo('Source go here')

@main.command()
def init():
    click.echo('init go here')

@main.command()
def clean():
    click.echo('clean go here')




def main_complex(args=None):
    # Generate a task config 
    task_config = TaskConfig(args)

    # Select a task
    # N.B. The task selection logic is tightly coupled to the arg parsing logic.
    # This task selection method is temporary until CT-208 is complete
    task_class = task_config.args.cls

    # Instantiate the task
    task = task_class(
        task_config.args, 
        task_config.config
        )

    # Set up logging
    # N.B. Logbook warnings are ignored from the CLI so we don't have to fork it to support 
    # python 3.10.
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")
    log_cache_events(getattr(task_config.args, "log_cache_events", False))
    log_path = getattr(task.config, "log_path", None)
    log_manager.set_path(log_path)
    task.set_log_format()
    setup_event_logger(
        log_path or "logs", 
        task.pre_init_hook(task_config.args)
    )

    # Prepare task run contexts
    task_run_contexts = [
        log_manager.applicationbound(),
        #adapter_management(),
        #track_run(task),
    ]
    #if task_config.args.record_timing_info:
        #task_run_contexts.append(
        #    profiler(
        #        outfile=task_config.args.record_timing_info
        #    )
        #)
    
    # Run the task in an ExitStack of contexts
    with ExitStack() as stack:
        for context in task_run_contexts:
            stack.enter_context(context)

        # Fire task start events
        #fire_event(MainReportVersion(v=str(dbt.version.installed)))
        #fire_event(MainReportArgs(args=args_to_dict(task_config.args)))
        #fire_event(MainTrackingUserState(user_state=dbt.tracking.active_user.state()))

        try:
            task_succeeded = task.run()
            exit_code = ExitCodes.Success.value if task_succeeded else ExitCodes.ModelError.value
        # Handle keyboard inturrupts
        except KeyboardInterrupt:
            fire_event(MainKeyboardInterrupt())
            exit_code = ExitCodes.UnhandledError.value
        # Handle system exits
        except SystemExit as e:
            exit_code = e.code
        # Handle other exceptions
        except BaseException as e: 
            fire_event(MainEncounteredError(e=str(e)))
            fire_event(MainStackTrace(stack_trace=traceback.format_exc()))
            exit_code = ExitCodes.UnhandledError.value
        finally:
            # Exit with approriate code
            sys.exit(exit_code)


#main = main_complex

@contextmanager
def adapter_management():
    reset_adapters()
    try:
        yield
    finally:
        cleanup_connections()

@contextmanager
def track_run(task):
    dbt.tracking.initialize_from_flags()
    dbt.tracking.track_invocation_start(config=task.config, args=task.args)
    try:
        yield
        dbt.tracking.track_invocation_end(config=task.config, args=task.args, result_type="ok")
    except (NotImplementedException, FailedToConnectException) as e:
        fire_event(MainEncounteredError(e=str(e)))
        dbt.tracking.track_invocation_end(config=task.config, args=task.args, result_type="error")
    except Exception:
        dbt.tracking.track_invocation_end(config=task.config, args=task.args, result_type="error")
        raise
    finally:
        dbt.tracking.flush()
