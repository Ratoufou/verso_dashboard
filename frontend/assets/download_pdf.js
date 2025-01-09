function explorePage(element, depth) {

    if (element.className.split(' ').find((cl) => cl === 'menu_item' || cl === 'menu') != undefined){}
    else if (element.className === 'dash-graph') {
        const graph_name = element.id;
        const graph = element.getElementsByClassName('js-plotly-plot')[0];

        if(graph_name === "graph-tab"){
            var height = 250;
            var width = 750;
        } else {
            var height = 600;
            var width = 1000;
        }

        return Plotly.toImage(graph, { format: 'png', width: width, height: height })
            .then(function(url) {
                const base64Image = url.split(',')[1];
                const filename = graph_name + '.png';

                return '    '.repeat(depth) 
                    + `<img src="data:image/png;base64,${base64Image}" alt="${filename}">\n` 
                    + '    '.repeat(depth) + '</img>\n';
            })
    }
    else if (element.id === 'yearslider'){} 
    else {

        const elem_str = element.outerHTML.split('<')[1];
        const elem_type = elem_str.split('>')[0].split(' ')[0];
        var html = '    '.repeat(depth) + `<${elem_str}\n`;
    
        if (element.children.length > 0) {
            const childrenPromises = Array.from(element.children).map(function(child) {
                return explorePage(child, depth + 1);
            });

            return Promise.all(childrenPromises)
                .then(function(childrenHtml) {
                    html += childrenHtml.join('');
                    html += "    ".repeat(depth) + `</${elem_type}>\n`;
                    return html;
                })
        }
    
        html += "    ".repeat(depth) + `</${elem_type}>\n`;
        return Promise.resolve(html);
    }
}


function generatePdf() {

    const assetsPromises = [
        fetch('assets/logo_bleu.png')
            .then(response => response.blob())
            .then(blob => URL.createObjectURL(blob)),

        fetch('assets/style.css')
            .then(response => response.text())
    ];

    const htmlContentPromise = explorePage(document.getElementById('dash_page'), 1);

    Promise.all([...assetsPromises, htmlContentPromise])
        .then(([logoUrl, cssContent, htmlContent]) => {
            var finalHtmlContent = 
            `<!DOCTYPE html>
<html>
<head>
<style>
${cssContent}
</style>
</head>
<body>
${htmlContent}
</body>
</html>`;

        finalHtmlContent = finalHtmlContent.replace('assets/logo_bleu.png', `${logoUrl}`)

        const options = {
            margin: [5, 5, 5, 5],
            filename: 'report.pdf',
            html2canvas: { scale: 4, windowWidth: 720 },
            jsPDF: { unit: 'mm', format: 'a4', orientation: 'portrait' },
        };

        html2pdf()
            .from(finalHtmlContent)
            .set(options)
            .save();
        })
}


setTimeout(function mainFunction(){
    try {
        document.getElementById("pdf-button").addEventListener("click", function(){
            generatePdf();
        })
      }
      catch(err) {
        console.log(err)
      }
    console.log("Listener Added!");
}, 5000);