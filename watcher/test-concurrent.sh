#!/bin/bash
# Test concurrent file processing

set -e

USERNAME=${1:-ole}
NUM_FILES=${2:-3}

echo "Testing concurrent processing for user: $USERNAME"
echo "Creating $NUM_FILES test files..."

# Create multiple test PDFs at once
for i in $(seq 1 $NUM_FILES); do
    FILENAME="test-concurrent-$i-$(date +%s).pdf"
    echo "Creating: $FILENAME"
    docker compose exec -T syncthing sh -c "echo 'test pdf content $i' > /sync/$USERNAME/incoming/$FILENAME" &
done

# Wait for all files to be created
wait

echo ""
echo "Files created. Watching logs for processing..."
echo "Look for 'active: N' to see concurrent processing"
echo ""

# Watch logs for the next 30 seconds
timeout 30 docker compose logs -f syncthing | grep -E "(Scheduled|Processing|Saved|active:)" || true

echo ""
echo "Test complete. Check if all $NUM_FILES files were processed."
