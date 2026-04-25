clear

GREEN="\033[0;32m"
RED="\033[0;31m"
NC="\033[0m"

find . -type d -name "node_modules" -prune -o -type f \( -name "*.js" -o -name "*.py" -o -name "*.json" \) -print | while read -r f; do
  case "$f" in
    *.js)
      if node --check "$f" 2>/dev/null; then
        printf "${GREEN}%s OK (JS)${NC}\n" "$f"
      else
        printf "${RED}%s FAIL (JS)${NC}\n" "$f"
      fi
      ;;
    *.py)
      if python3 -m py_compile "$f" 2>/dev/null; then
        printf "${GREEN}%s OK (PY)${NC}\n" "$f"
      else
        printf "${RED}%s FAIL (PY)${NC}\n" "$f"
      fi
      ;;
    *.json)
      if python3 -m json.tool "$f" > /dev/null 2>&1; then
        printf "${GREEN}%s OK (JSON)${NC}\n" "$f"
      else
        printf "${RED}%s FAIL (JSON)${NC}\n" "$f"
      fi
      ;;
  esac
done