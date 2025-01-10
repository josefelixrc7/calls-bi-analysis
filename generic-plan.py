# Clean segments
import functions.segments as s

s.CleanSegments()
s.CleanDatabasesUsed()
s.UseDatabase(16)

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
s.CreateSegment(7000, '85000')
