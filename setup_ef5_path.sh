#!/bin/bash

# Configuration file to update
CONFIG_FILE="Cuba_config.py"
YELLOW='\033[0;33m'
RESET='\033[0m'

echo "Searching for EF5 executable..."

# 1. Check PATH
if command -v ef5 &> /dev/null; then
    EF5_BIN=$(command -v ef5)
    echo "Found EF5 executable in PATH: $EF5_BIN"
else
    # 2. Check ~/EF5 directory
    EF5_BASE="$HOME/EF5"
    if [ -d "$EF5_BASE" ]; then
        # Find all 'ef5' files that are executable
        # We use 'find' to look for files named 'ef5' that are executable
        CANDIDATES=($(find "$EF5_BASE" -type f -name "ef5" -executable | sort | uniq))
        
        NUM_CANDIDATES=${#CANDIDATES[@]}
        
        if [ "$NUM_CANDIDATES" -eq 1 ]; then
            EF5_BIN="${CANDIDATES[0]}"
            echo "Found EF5 executable: $EF5_BIN"
        elif [ "$NUM_CANDIDATES" -gt 1 ]; then
            echo -e "${YELLOW}WARNING: Multiple EF5 executables found in $EF5_BASE:${RESET}"
            for c in "${CANDIDATES[@]}"; do
                echo " - $c"
            done
            
            # Try to pick 'EF5LatestRelease' automatically
            EF5_BIN=""
            for c in "${CANDIDATES[@]}"; do
                if [[ "$c" == *"EF5LatestRelease"* ]]; then
                    EF5_BIN="$c"
                    break
                fi
            done
            
            if [ -n "$EF5_BIN" ]; then
                echo "Auto-selecting likely candidate: $EF5_BIN"
            else
                echo -e "${YELLOW}Could not determine which EF5 to use automatically.${RESET}"
                echo -e "${YELLOW}Please set 'ef5Path' manually in $CONFIG_FILE${RESET}"
                exit 1
            fi
        else
             echo -e "${YELLOW}WARNING: EF5 executable not found automatically in PATH or $EF5_BASE${RESET}"
             echo -e "${YELLOW}Please set 'ef5Path' manually in $CONFIG_FILE${RESET}"
             exit 1
        fi
    else
        echo -e "${YELLOW}WARNING: $EF5_BASE directory does not exist.${RESET}"
        echo -e "${YELLOW}Please set 'ef5Path' manually in $CONFIG_FILE${RESET}"
        exit 1
    fi
fi

# Update the config file if we found a binary
if [ -n "$EF5_BIN" ]; then
    if [ -f "$CONFIG_FILE" ]; then
        # Escape slashes for sed
        ESCAPED_PATH=$(echo "$EF5_BIN" | sed 's/\//\\\//g')
        
        # Check if the line exists
        if grep -q "ef5Path =" "$CONFIG_FILE"; then
            # Replace the line using sed
            sed -i "s/^ef5Path = .*/ef5Path = \"$ESCAPED_PATH\"/" "$CONFIG_FILE"
            echo "Successfully updated $CONFIG_FILE with ef5Path = \"$EF5_BIN\""
        else
            echo "Could not find 'ef5Path =' line in $CONFIG_FILE to update."
        fi
    else
        echo "Error: $CONFIG_FILE not found."
    fi
fi
