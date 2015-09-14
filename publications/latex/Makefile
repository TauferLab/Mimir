all:
	pdflatex paper
	#bibtex paper
	pdflatex paper
	pdflatex paper
	#dvips -o paper.ps paper
	#ps2pdf -dCompatibilityLevel=1.4 -dOptimize=true -dEmbedAllFonts=true paper.ps paper.pdf

clean:
	rm -rf *.aux  *.blg *.dvi *.log *.pdf *~
#	rm *.ps
