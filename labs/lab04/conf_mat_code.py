from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.types import FloatType


def printConfMatrix(DF, title):
    preds_and_labels = DF.select(['prediction','label'])\
                    .withColumn('label', DF.label.cast(FloatType()))\
                    .orderBy('prediction')
    # This RDD contains tuples of prediction and label
    predictionAndLabels = preds_and_labels\
                .select(['prediction','label']).rdd.map(tuple)   
    # Needs to be applied on an RDD 
    metrics = MulticlassMetrics(predictionAndLabels)       
    # Produce confusion matrix
    cm = metrics.confusionMatrix().toArray()      
    classes = DF.select("class:207").distinct()\
                .rdd.map(lambda r: r[0]).collect()
    classes = [el.replace("class:", "") for el in classes] 
    fig, ax = plt.subplots(figsize =(10, 8))
    fontsize = 15
    ax = sns.heatmap(cm, xticklabels=classes, yticklabels=classes,\
                     linewidth = 0.2,cmap="BuPu", annot = True, \
                     fmt = ".2f", annot_kws={"fontsize":fontsize-2})
    cbar = ax.collections[0].colorbar 
    cbar.ax.tick_params(labelsize=fontsize) 
    ax.figure.axes[-1].yaxis.label.set_size(fontsize+5) 
    ax.figure.axes[-1].yaxis.set_label_coords(3,.5) 
    ax.set_xticklabels(classes, fontsize=fontsize, rotation = 90) 
    ax.set_yticklabels(classes, fontsize=fontsize, rotation = 0) 
    ax.set_ylabel("True class", fontsize = fontsize + 5) 
    ax.set_xlabel("Predicted class", fontsize = fontsize + 5) 
    ax.yaxis.set_label_coords(-.22,.3) 
    ax.xaxis.set_label_coords(.5, -.3)
    ax.set_title(title)
    plt.tight_layout()
    plt.show()

    
if __name__ == "__main__":
    print("Hello")