def write_output(validation_type, source_name, target_name, no_of_source_record_count, no_of_target_record_count
                 , failed_count, column, status, source_type, target_type, out):
    out["validation_type"].append(validation_type)
    out["source_name"].append(source_name)
    out["target_name"].append(target_name)
    out["no_of_source_record_count"].append(no_of_source_record_count)
    out["no_of_target_record_count"].append(no_of_target_record_count)
    out["failed_count"].append(failed_count)
    out["column"].append(column)
    out["status"].append(status)
    out["source_type"].append(source_type)
    out["target_type"].append(target_type)
