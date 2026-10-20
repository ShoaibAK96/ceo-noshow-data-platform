
"""Repository-only heuristic scan; does not replace a human confidentiality review."""
import ipaddress,re,subprocess
from pathlib import Path
root=Path(__file__).resolve().parents[1]
tracked=subprocess.check_output(['git','ls-files','-z'],cwd=root).decode().split('\0')
issues=[]
# Split tokens avoid embedding real internal labels into this public scanner.
for name in filter(None,tracked):
 p=root/name
 if p.suffix.lower() in ['.png']:continue
 text=p.read_text()
 for pattern in [r'(?i)\b(?:password|api_key|access_token)\s*=\s*[\x22\x27][^\x22\x27]+',r'-----BEGIN [A-Z ]*PRIVATE KEY-----',r'gh[pousr]_[A-Za-z0-9]{20,}',r'AKIA[0-9A-Z]{16}',r'\\\\[a-zA-Z0-9_-]+\\',r'[A-Za-z]:\\[A-Za-z]']:
  if re.search(pattern,text):issues.append((name,'sensitive-pattern'))
 for ip in re.findall(r'(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])',text):
  try:
   address=ipaddress.ip_address(ip)
   if address in ipaddress.ip_network((10 << 24, 8)) or address in ipaddress.ip_network(((172 << 24) + (16 << 16), 12)) or address in ipaddress.ip_network(((192 << 24) + (168 << 16), 16)):issues.append((name,'private-IP'))
  except ValueError:pass
assert not issues,issues
print('PASS: heuristic secret, private-IP and infrastructure-path scan over tracked files')
