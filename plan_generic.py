
print('Use DB: ', end="")
use_db = input()
print('Records cantity: ', end="")
cantity = int(input())
print('Segment name: ', end="")
segment_name = input()

# Clean segments
import functions.segments as s

s.CleanSegments()

# Clean DBs used
s.CleanDatabasesUsed()

# Use DB
s.UseDatabase(use_db)

# Exclusions
import functions.exclusions as e

e.ExcludeBacklist()
e.ExcludeSales()
e.ExcludeOverused()
e.ExcludeNoreusable()
e.ExcludeNoDuration

# Analysis
import functions.analysis
import functions.segments as s

a = functions.analysis.Analysis()

s.SegmentLeft()

s.CreateSegment(cantity, segment_name)

# Export Analysis
import functions.analysis as a

analysis = a.Analysis()
analysis.Export()