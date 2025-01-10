# Clean segments
import functions.segments as s

s.CleanSegments()

# Clean DBs used
import functions.segments as s

s.CleanDatabasesUsed()

# Use DB
import functions.segments as s

s.UseDatabase(1) # Castor

# Exclusions
import functions.exclusions as e

e.ExcludeBacklist()
e.ExcludeSales()
e.ExcludeOverused()
e.ExcludeNoreusable()
e.ExcludeNoDuration()

# Analysis
import functions.analysis
import functions.segments as s

a = functions.analysis.Analysis()

a.AnalysisMinutos1Plus()
s.CreateSegment(15000, '61032')
#s.CreateSegment(15000, '61033')

a.AnalysisNoreste()
s.CreateSegment(15000, '61034')

a.AnalysisNoroeste()
s.CreateSegment(15000, '61035')
#a.AnalysisOccidente()
#s.CreateSegment(5000, '61036')

#a.AnalysisEFE20()
#s.CreateSegment(15000, '61044')

#a.AnalysisReferidos()
#s.CreateSegment(5000, '61046')

# Export Analysis
import functions.analysis as a

analysis = a.Analysis()
analysis.Export()