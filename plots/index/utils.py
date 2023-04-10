
import os

def savefig(plt, name):
    plt.savefig(f'{name}.pdf', format='pdf', bbox_inches='tight')
    os.system(f'pdfcrop {name}.pdf')
    os.system(f'mv {name}-crop.pdf {name}.pdf')
