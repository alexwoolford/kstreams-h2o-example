import hex.genmodel.ModelMojoReader;
import hex.genmodel.MojoModel;
import hex.genmodel.MojoReaderBackend;
import hex.genmodel.MojoReaderBackendFactory;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.MultinomialModelPrediction;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;

import static org.testng.Assert.assertEquals;

public class IrisClassifierTest {

    @Test
    private void testClassifier() throws IOException, PredictException {

        URL mojoSource = getClass().getClassLoader().getResource("DeepLearning_grid_1_AutoML_20190610_224939_model_2.zip");
        MojoReaderBackend reader = MojoReaderBackendFactory.createReaderBackend(mojoSource, MojoReaderBackendFactory.CachingStrategy.MEMORY);
        MojoModel model = ModelMojoReader.readFrom(reader);
        EasyPredictModelWrapper modelWrapper = new EasyPredictModelWrapper(model);


        RowData versicolorRow = new RowData();
        versicolorRow.put("sepal_length", 6.1);
        versicolorRow.put("sepal_width", 2.9);
        versicolorRow.put("petal_length", 4.7);
        versicolorRow.put("petal_width", 1.4);

        MultinomialModelPrediction versicolorPrediction = (MultinomialModelPrediction) modelWrapper.predict(versicolorRow);
        assertEquals(versicolorPrediction.label, "versicolor");


        RowData setosaRow = new RowData();
        setosaRow.put("sepal_length", 5.0);
        setosaRow.put("sepal_width", 3.6);
        setosaRow.put("petal_length", 1.4);
        setosaRow.put("petal_width", 0.2);

        MultinomialModelPrediction setosaPrediction = (MultinomialModelPrediction) modelWrapper.predict(setosaRow);
        assertEquals(setosaPrediction.label, "setosa");


        RowData virginicaRow = new RowData();
        virginicaRow.put("sepal_length", 6.5);
        virginicaRow.put("sepal_width", 3.2);
        virginicaRow.put("petal_length", 5.1);
        virginicaRow.put("petal_width", 2.0);

        MultinomialModelPrediction virginicaPrediction = (MultinomialModelPrediction) modelWrapper.predict(virginicaRow);
        assertEquals(virginicaPrediction.label, "virginica");

    }

}
