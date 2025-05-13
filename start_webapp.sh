#!/bin/bash

# Script to start backend and frontend development servers

# Define the base directory (where this script is located)
BASE_DIR=$(pwd) # Or you can hardcode it: BASE_DIR="/path/to/your/WebApp"

# Define child directory names
BACKEND_DIR="WebApp/backend"
FRONTEND_DIR="WebApp/frontend"

# --- Output Color Functions (Optional, for better readability) ---
color_green() {
  echo -e "\033[32m$1\033[0m"
}

color_blue() {
  echo -e "\033[34m$1\033[0m"
}

color_red() {
  echo -e "\033[31m$1\033[0m"
}

# --- Check if directories exist ---
if [ ! -d "$BASE_DIR/$BACKEND_DIR" ]; then
  color_red "Error: Backend directory '$BASE_DIR/$BACKEND_DIR' not found."
  exit 1
fi

if [ ! -d "$BASE_DIR/$FRONTEND_DIR" ]; then
  color_red "Error: Frontend directory '$BASE_DIR/$FRONTEND_DIR' not found."
  exit 1
fi

# --- Start Backend Server ---
color_green "Starting Backend Server in '$BACKEND_DIR'..."
( # Start in a subshell to run in the background and allow script to continue
  cd "$BASE_DIR/$BACKEND_DIR" || { color_red "Failed to cd into $BACKEND_DIR"; exit 1; }
  echo "Current directory: $(pwd)"
  echo "Running: node server.js"
  node server.js & # Run in the background
  BACKEND_PID=$! # Get the Process ID of the last backgrounded command
  echo "Backend server started with PID: $BACKEND_PID"
)

# Give the backend a moment to start up (optional, adjust as needed)
sleep 2

# --- Start Frontend Development Server ---
color_green "Starting Frontend Development Server in '$FRONTEND_DIR'..."
( # Start in a subshell
  cd "$BASE_DIR/$FRONTEND_DIR" || { color_red "Failed to cd into $FRONTEND_DIR"; exit 1; }
  echo "Current directory: $(pwd)"
  echo "Running: npm run dev"
  npm run dev # This will likely run in the foreground and take over this terminal tab/window
              # OR if it also backgrounds, get its PID:
              # npm run dev &
              # FRONTEND_PID=$!
              # echo "Frontend dev server started with PID: $FRONTEND_PID"
)

# --- Optional: Wait for processes or provide instructions to stop ---
# If npm run dev runs in foreground, this part won't be reached until it's stopped.
echo ""
color_blue "Backend server (PID: $BACKEND_PID) is running in the background."
color_blue "Frontend dev server is likely running in the foreground in its own terminal or tab if started separately."
color_blue "To stop the backend server, use: kill $BACKEND_PID"
color_blue "To stop the frontend server, use Ctrl+C in its terminal."

# If you want the script to wait for the background process to be manually killed (less common for dev servers)
# wait $BACKEND_PID
# echo "Backend server stopped."