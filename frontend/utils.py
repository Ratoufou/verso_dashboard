def color_gradient(color1, color2, n):
    return [tuple([int(color1[j] + (color2[j] - color1[j])*i/(n-1)) 
                   for j in range(3)]) 
            for i in range(n)]