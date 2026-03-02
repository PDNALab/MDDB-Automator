#!/bin/bash
# MDDB Automation Script
# This script monitors for new MD systems and automates analysis and upload

# Configuration
MDDB_DIR="/orange/alberto.perezant-mddb/MDDB"
LOG_DIR="/orange/alberto.perezant-mddb/mddb_automation/automation_logs"
RECORD_FILE="${LOG_DIR}/uploaded_systems.txt"
FAILED_FILE="${LOG_DIR}/failed_systems.txt"
TEMP_FAILED_FILE="${LOG_DIR}/failed_systems_temp.txt"
LOCK_FILE="${LOG_DIR}/automation.lock"
ACCESSION_FILE="/orange/alberto.perezant-mddb/mddb_automation/all_accession.dat"
REMOTE_HOST="pubperez1"
REMOTE_USER="perez"
REMOTE_PATH="/pubapps/perez/mddb/data"
LOG_FILE="${LOG_DIR}/automation_$(date +%Y%m%d_%H%M%S).log"
MAX_LOG_FILES=5

# Supported file formats
TOPOLOGY_FORMATS=("*.prmtop" "*.parm7" "*.psf" "*.top")
TRAJECTORY_FORMATS=("*.dcd" "*.xtc" "*.trr" "*.nc" "*.netcdf")

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Function to clean old log files (keep only latest 5)
clean_old_logs() {
    log_message "Cleaning old log files, keeping latest ${MAX_LOG_FILES}"
    cd "$LOG_DIR"
    ls -t automation_*.log 2>/dev/null | tail -n +$((MAX_LOG_FILES + 1)) | xargs -r rm -f
}

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to check if another instance is running
check_lock() {
    if [ -f "$LOCK_FILE" ]; then
        PID=$(cat "$LOCK_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            log_message "Another instance is running (PID: $PID). Exiting."
            exit 1
        else
            log_message "Stale lock file found. Removing."
            rm -f "$LOCK_FILE"
        fi
    fi
    echo $$ > "$LOCK_FILE"
}

# Function to remove lock on exit
cleanup() {
    rm -f "$LOCK_FILE"
    log_message "Script completed."
    clean_old_logs
}
trap cleanup EXIT

# Initialize record files if they don't exist
touch "$RECORD_FILE"
touch "$FAILED_FILE"

# Function to check if system is already processed
is_processed() {
    local system_name="$1"
    grep -q "^${system_name}$" "$RECORD_FILE"
    return $?
}

# Function to get highest accession number from all_accession.dat
get_next_accession() {
    if [ ! -f "$ACCESSION_FILE" ]; then
        echo "A0001" > "$ACCESSION_FILE"
        echo "A0001"
        return 0
    fi
    
    local highest=$(grep -oE 'A[0-9][0-9][0-9][0-9]' "$ACCESSION_FILE" | sort -u | tail -n 1)
    
    if [ -z "$highest" ]; then
        echo "A0001"
    else
        local num=$(echo "$highest" | sed 's/A//')
        local next_num=$((10#$num + 1))
        printf "A%04d" "$next_num"
    fi
}

# Function to extract accession ID from system name
get_accession_id() {
    local system_name="$1"
    echo "$system_name" | grep -oE 'A[0-9][0-9][0-9][0-9]' | tail -n 1
}

# Function to validate accession in inputs.yaml
validate_accession_in_yaml() {
    local system_path="$1"
    local expected_accession="$2"
    local yaml_file="${system_path}/inputs.yaml"
    
    if [ ! -f "$yaml_file" ]; then
        return 1
    fi
    
    local yaml_accession=$(grep -oE '^accession:[[:space:]]*A[0-9][0-9][0-9][0-9]' "$yaml_file" | grep -oE 'A[0-9][0-9][0-9][0-9]')
    
    if [ -z "$yaml_accession" ]; then
        sed -i "1i accession: ${expected_accession}" "$yaml_file"
        return 0
    fi
    
    if [ "$yaml_accession" != "$expected_accession" ]; then
        sed -i "s/^accession:[[:space:]]*A[0-9][0-9][0-9][0-9]/accession: ${expected_accession}/" "$yaml_file"
    fi
    
    return 0
}

# Function to validate and fix folder naming
validate_and_fix_folder_name() {
    local system_path="$1"
    local system_name=$(basename "$system_path")
    
    if echo "$system_name" | grep -qE '^[^_]+_[^_]+_[^_]+_A[0-9][0-9][0-9][0-9]$'; then
        echo "$system_name"
        return 0
    else
        local next_accession=$(get_next_accession)
        local new_name="${system_name}_${next_accession}"
        local new_path="${MDDB_DIR}/${new_name}"
        
        if mv "$system_path" "$new_path" 2>/dev/null; then
            echo "$next_accession" >> "$ACCESSION_FILE"
            echo "$new_name"
            return 0
        else
            echo "$system_name"
            return 1
        fi
    fi
}

# Function to find topology file
find_topology_file() {
    local system_path="$1"
    local topology=""
    
    for format in "${TOPOLOGY_FORMATS[@]}"; do
        topology=$(find "${system_path}/raw/" -name "$format" -type f 2>/dev/null | head -n 1)
        if [ -n "$topology" ]; then
            echo "$topology"
            return 0
        fi
    done
    
    return 1
}

# Function to find trajectory files
find_trajectory_files() {
    local system_path="$1"
    local found=false
    
    for format in "${TRAJECTORY_FORMATS[@]}"; do
        if ls "${system_path}/raw/"$format &> /dev/null; then
            found=true
            break
        fi
    done
    
    if [ "$found" = false ]; then
        return 1
    fi
    
    return 0
}

# Function to count trajectories (supports multiple formats)
count_trajectories() {
    local system_path="$1"
    local count=0
    
    for format in "${TRAJECTORY_FORMATS[@]}"; do
        local format_count=$(ls "${system_path}/raw/"$format 2>/dev/null | wc -l)
        count=$((count + format_count))
    done
    
    echo "$count"
}

# Function to validate system folder structure
validate_system() {
    local system_path="$1"
    local system_name=$(basename "$system_path")
    
    if [ ! -d "${system_path}/raw" ]; then
        log_message "ERROR: ${system_name} - Missing 'raw' folder"
        return 1
    fi
    
    if [ ! -f "${system_path}/inputs.yaml" ]; then
        log_message "ERROR: ${system_name} - Missing 'inputs.yaml' file"
        return 1
    fi
    
    local topology=$(find_topology_file "$system_path")
    if [ -z "$topology" ]; then
        log_message "ERROR: ${system_name} - No topology file found in raw folder"
        log_message "Supported formats: ${TOPOLOGY_FORMATS[*]}"
        return 1
    fi
    log_message "Found topology file: $(basename $topology)"
    
    if ! find_trajectory_files "$system_path"; then
        log_message "ERROR: ${system_name} - No trajectory files found in raw folder"
        log_message "Supported formats: ${TRAJECTORY_FORMATS[*]}"
        return 1
    fi
    
    return 0
}

# Function to create SLURM job script
create_slurm_script() {
    local system_path="$1"
    local system_name="$2"
    local num_trajectories="$3"
    local script_path="${system_path}/run_analysis.slurm"
    
    cat > "$script_path" << 'SLURM_EOF'
#!/bin/bash
#SBATCH --job-name=MDDB_AUTO
#SBATCH --output=MDDB.out
#SBATCH --error=MDDB.err
#SBATCH --mail-type=FAIL
##SBATCH --mail-user=<email@ufl.edu>
##SBATCH --partition=hpg-b200
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=8
##SBATCH --gpus-per-task=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=100000mb
#SBATCH --time=5:00:00
#SBATCH --qos=alberto.perezant-b

SYSTEM_PATH="$1"
NUM_TRAJ="$2"

ml conda
source /apps/conda/25.7.0/etc/profile.d/conda.sh
conda activate /orange/alberto.perezant/imesh.ranaweera/.conda/envs/mwf_env

cd "$SYSTEM_PATH"

# Find topology file (support multiple formats)
TOPOLOGY=""
for format in prmtop parm7 psf top; do
    TOPOLOGY=$(find raw/ -name "*.${format}" -type f | head -n 1)
    if [ -n "$TOPOLOGY" ]; then
        echo "Using topology file: $TOPOLOGY"
        break
    fi
done

if [ -z "$TOPOLOGY" ]; then
    echo "ERROR: Could not find topology file"
    exit 1
fi

# Run analysis for each trajectory
for i in $(seq 0 $((NUM_TRAJ-1)))
do
    idx=$(printf "%02d" $i)
    echo "Running MDDB workflow for replica_$idx"
    
    TRAJ=""
    for ext in dcd xtc trr nc netcdf; do
        for pattern in "*.$idx.$ext" "*_$idx.$ext" "trajectory.$idx.$ext" "*.$i.$ext" "*_$i.$ext" "trajectory.$i.$ext"; do
            TRAJ=$(find raw/ -name "$pattern" -type f | head -n 1)
            if [ -n "$TRAJ" ]; then
                echo "Found trajectory: $TRAJ"
                break 2
            fi
        done
    done
    
    if [ -z "$TRAJ" ]; then
        echo "WARNING: Trajectory file for replica $idx not found"
        continue
    fi
    
    mwf run -e tmscore \
        -dir "$SYSTEM_PATH" \
        -top "$TOPOLOGY" \
        -md replica_$idx "$TRAJ" \
        -inp inputs.yaml -ns -fit \
        -m stabonds cohbonds \
        -filt
    
    if [ $? -eq 0 ]; then
        echo "Successfully completed analysis for replica_$idx"
    else
        echo "ERROR: Analysis failed for replica_$idx"
    fi
done

echo "Done!"
SLURM_EOF

    sed -i "s|\$1|${system_path}|g" "$script_path"
    sed -i "s|\$2|${num_trajectories}|g" "$script_path"
    
    echo "$script_path"
}

# Function to run analysis
run_analysis() {
    local system_path="$1"
    local system_name="$2"
    local num_trajectories="$3"
    
    log_message "Creating SLURM script for ${system_name}"
    local slurm_script=$(create_slurm_script "$system_path" "$system_name" "$num_trajectories")
    
    log_message "Submitting analysis job for ${system_name}"
    cd "$system_path"
    
    JOB_ID=$(sbatch "$slurm_script" 2>&1 | awk '{print $NF}')
    
    if [ -z "$JOB_ID" ]; then
        log_message "ERROR: Failed to submit job for ${system_name}"
        return 1
    fi
    
    log_message "Job submitted with ID: ${JOB_ID} for ${system_name}"
    return 0
}

# Function to check if analysis is complete
check_analysis_complete() {
    local system_path="$1"
    local output_file="${system_path}/MDDB.out"
    
    if [ ! -f "$output_file" ]; then
        return 1
    fi
    
    if grep -q "Done!" "$output_file"; then
        return 0
    fi
    
    return 1
}

# Function to transfer files (DIRECT connection - no gateway)
transfer_files() {
    local system_path="$1"
    local system_name="$2"
    
    log_message "Transferring files for ${system_name} to ${REMOTE_HOST}"
    cd "$MDDB_DIR"
    
    # Transfer files directly to pubperez1 (no gateway needed)
    rsync -avP --exclude 'raw/' \
        "${system_name}/" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}/${system_name}/" \
        >> "$LOG_FILE" 2>&1
    
    return $?
}

# Function to upload to MDDB (DIRECT connection - no gateway)
upload_to_mddb() {
    local system_name="$1"
    local accession_id="$2"
    
    log_message "Loading data to Florida node for ${system_name}"
    
    # SSH directly to pubperez1 (no gateway needed)
    ssh "${REMOTE_USER}@${REMOTE_HOST}" << EOF >> "$LOG_FILE" 2>&1
cd ${REMOTE_PATH}
podman run --rm --network data_network -v ${REMOTE_PATH}:/data:Z localhost/loader_image load /data/${system_name}
EOF
    
    if [ $? -ne 0 ]; then
        log_message "ERROR: Failed to load data for ${system_name}"
        return 1
    fi
    
    log_message "Publishing to main node for ${system_name} (Accession: ${accession_id})"
    
    ssh "${REMOTE_USER}@${REMOTE_HOST}" << EOF >> "$LOG_FILE" 2>&1
podman run --rm --network data_network -v ${REMOTE_PATH}:/data:Z localhost/loader_image publish ${accession_id}
EOF
    
    if [ $? -ne 0 ]; then
        log_message "ERROR: Failed to publish data for ${system_name}"
        return 1
    fi
    
    return 0
}

# Main processing loop
main() {
    log_message "Starting MDDB automation check"
    check_lock
    
    # Create temporary failed file for this run
    > "$TEMP_FAILED_FILE"
    
    # Find all system directories
    for system_path in "${MDDB_DIR}"/*/ ; do
        [ -d "$system_path" ] || continue
        
        local original_name=$(basename "$system_path")
        
        # Skip automation_logs directory
        if [ "$original_name" = "automation_logs" ] || [ "$original_name" = "mddb_automation" ]; then
            continue
        fi
        
        log_message "Processing: ${original_name}"
        
        # Validate and fix folder name if needed
        local system_name=$(validate_and_fix_folder_name "$system_path")
        local rename_status=$?
        
        if [ $rename_status -ne 0 ]; then
            log_message "ERROR: Failed to validate/fix folder name for ${original_name}"
            echo "${original_name} - Folder naming failed" >> "$TEMP_FAILED_FILE"
            continue
        fi
        
        # Log if renamed
        if [ "$system_name" != "$original_name" ]; then
            log_message "Renamed ${original_name} to ${system_name}"
        else
            log_message "Folder name ${system_name} is correctly formatted"
        fi
        
        # Update system_path if name was changed
        system_path="${MDDB_DIR}/${system_name}"
        
        # Check if already processed
        if is_processed "$system_name"; then
            log_message "System ${system_name} already processed. Skipping."
            continue
        fi
        
        log_message "Found new system: ${system_name}"
        
        # Extract and validate accession ID
        local accession_id=$(get_accession_id "$system_name")
        if [ -z "$accession_id" ]; then
            log_message "ERROR: Could not extract accession ID from ${system_name}"
            echo "${system_name} - Invalid accession ID" >> "$TEMP_FAILED_FILE"
            continue
        fi
        
        log_message "Accession ID: ${accession_id}"
        
        # Validate accession in inputs.yaml
        if ! validate_accession_in_yaml "$system_path" "$accession_id"; then
            log_message "ERROR: Failed to validate accession in inputs.yaml for ${system_name}"
            echo "${system_name} - YAML validation failed" >> "$TEMP_FAILED_FILE"
            continue
        fi
        
        log_message "Accession validated in inputs.yaml"
        
        # Validate system structure
        if ! validate_system "$system_path"; then
            echo "${system_name} - Validation failed" >> "$TEMP_FAILED_FILE"
            continue
        fi
        
        # Count trajectories
        local num_trajectories=$(count_trajectories "$system_path")
        log_message "${system_name} has ${num_trajectories} trajectories"
        
        if [ "$num_trajectories" -eq 0 ]; then
            log_message "ERROR: No trajectories found for ${system_name}"
            echo "${system_name} - No trajectories" >> "$TEMP_FAILED_FILE"
            continue
        fi
        
        # Check if analysis has been completed
        if ! check_analysis_complete "$system_path"; then
            log_message "Analysis not complete for ${system_name}. Checking for running jobs..."
            
            # Check if there's already a job running for this system
            if squeue -u "$USER" -n "MDDB_AUTO" -h 2>/dev/null | grep -q "MDDB_AUTO"; then
                log_message "Analysis job already running for MDDB systems. Skipping."
                continue
            fi
            
            # Run analysis
            if ! run_analysis "$system_path" "$system_name" "$num_trajectories"; then
                echo "${system_name} - Analysis submission failed" >> "$TEMP_FAILED_FILE"
                continue
            fi
            
            log_message "Analysis submitted for ${system_name}. Will process in next run."
            continue
        fi
        
        log_message "Analysis complete for ${system_name}. Proceeding with upload."
        
        # Transfer files
        if ! transfer_files "$system_path" "$system_name"; then
            log_message "ERROR: File transfer failed for ${system_name}"
            echo "${system_name} - Transfer failed" >> "$TEMP_FAILED_FILE"
            continue
        fi
        
        # Upload to MDDB
        if ! upload_to_mddb "$system_name" "$accession_id"; then
            log_message "ERROR: Upload failed for ${system_name}"
            echo "${system_name} - Upload failed" >> "$TEMP_FAILED_FILE"
            continue
        fi
        
        # Mark as processed
        echo "${system_name}" >> "$RECORD_FILE"
        log_message "Successfully processed and uploaded ${system_name}"
    done
    
    # Check if there were any failures in this run
    if [ -s "$TEMP_FAILED_FILE" ]; then
        # There were failures - replace the failed_systems.txt with new failures
        mv "$TEMP_FAILED_FILE" "$FAILED_FILE"
        log_message "=== FAILED SYSTEMS ==="
        cat "$FAILED_FILE" | tee -a "$LOG_FILE"
    else
        # No failures - clear the failed_systems.txt
        > "$FAILED_FILE"
        rm -f "$TEMP_FAILED_FILE"
        log_message "No failures in this run. Cleared failed_systems.txt"
    fi
    
    log_message "Automation check completed"
}

# Run main function
main