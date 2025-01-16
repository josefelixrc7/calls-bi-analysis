# Clean segments
import functions.segments as s

s.CleanSegments()

# Clean DBs used
s.CleanDatabasesUsed()

# Use DB
print('Use DB: ', end="")
s.UseDatabase(input())

# Exclusions
import functions.exclusions as e

e.ExcludeBacklist()
e.ExcludeSales()
e.ExcludeOverused()

# Analysis
import functions.analysis
import functions.segments as s

a = functions.analysis.Analysis()

s.SegmentLeft()

print('Records cantity: ', end="")
cantity = int(input())
print('Segment name: ', end="")
segment_name = input()
s.CreateSegment(cantity, segment_name)

# Export Analysis
import functions.analysis as a

analysis = a.Analysis()
analysis.Export()