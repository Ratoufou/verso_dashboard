// function explorePage(element, depth, zip) {

//     if (element.className === 'dash-graph') {
//         const graph_name = element.id;
//         const graph = element.getElementsByClassName('js-plotly-plot')[0];

//         return Plotly.toImage(graph, { format: 'png' })
//             .then(function(url) {
//                 const base64Image = url.split(',')[1];
//                 const filename = graph_name + '.png';

//                 return '    '.repeat(depth) 
//                     + `<img src="data:image/png;base64,${base64Image}" alt="${filename}">\n` 
//                     + '    '.repeat(depth) + '</img>\n';
//             })
//     } 
//     else if (element.id === 'yearslider'){} 
//     else {

//         const elem_str = element.outerHTML.split('<')[1];
//         const elem_type = elem_str.split('>')[0].split(' ')[0];
//         var html = '    '.repeat(depth) + `<${elem_str}\n`;
    
//         if (element.children.length > 0){
//             const childrenPromises = Array.from(element.children).map(function(child) {
//                 return explorePage(child, depth + 1, zip);
//             });

//             return Promise.all(childrenPromises)
//                 .then(function(childrenHtml) {
//                     html += childrenHtml.join('');
//                     html += "    ".repeat(depth) + `</${elem_type}>\n`;
//                     return html;
//                 })
//         }
    
//         html += "    ".repeat(depth) + `</${elem_type}>\n`;
//         return Promise.resolve(html);
//     }
// }


// function saveZip() {

//     const zip = new JSZip();

//     fetch('assets/logo_bleu.png')
//         .then(response => response.blob())
//         .then(blob => {
//             zip.folder('assets').file('logo_bleu.png', blob);
//         })

//     fetch('assets/style.css')
//         .then(response => response.text())
//         .then(cssContent => {
//             zip.folder('assets').file('style.css', cssContent);
//         })

//     const htmlContentPromise = explorePage(document.getElementById('dash_page'), 1, zip);

//     htmlContentPromise
//         .then(htmlContent => {
//             const finalHtmlContent = `<!DOCTYPE html>
// <html>
// <head>
//     <link rel="stylesheet" href="assets/style.css">
// </head>
// <body>
// ${htmlContent}
// </body>
// </html>`;

//         zip.file('index.html', finalHtmlContent);

//         zip.generateAsync({ type: "blob" })
//             .then(function(zipBlob) {
//                 const a = document.createElement("a");
//                 a.href = URL.createObjectURL(zipBlob);
//                 a.download = "page.zip";
//                 a.click();
//                 URL.revokeObjectURL(a.href);
//             })
//         })
// }


// setTimeout(function mainFunction(){
//     try {
//         document.getElementById("pdf-button").addEventListener("click", function(){
//             saveZip();
//         })
//       }
//       catch(err) {
//         console.log(err)
//       }
//     console.log("Listener Added!");
// }, 5000);