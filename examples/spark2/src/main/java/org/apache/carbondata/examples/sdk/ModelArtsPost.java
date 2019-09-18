//package org.apache.carbondata.examples.sdk;
//
//import java.io.IOException;
//import java.util.Map;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//
//import okhttp3.Call;
//import okhttp3.Callback;
//import okhttp3.MediaType;
//import okhttp3.OkHttpClient;
//import okhttp3.Request;
//import okhttp3.RequestBody;
//import okhttp3.Response;
//import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
//import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
//import org.python.antlr.ast.Str;
//
//public class ModelArtsPost {
//
//  public static void main(String[] args) {
//    userLogin("hwstaff_l00215684", "@Huawei123");
//  }
//
//  private static void userLogin(String username, String password) {
//    String loginRequest = "{\n" +
//        "  \"auth\": {\n" +
//        "    \"identity\": {\n" +
//        "      \"methods\": [\"password\"],\n" +
//        "      \"password\": {\n" +
//        "        \"user\": {\n" +
//        "          \"name\": \"" + username + "\",\n" +
//        "          \"password\": \"" + password + "\",\n" +
//        "          \"domain\": {\n" +
//        "            \"name\": \"" + username + "\"\n" +
//        "          }\n" +
//        "        }\n" +
//        "      }\n" +
//        "    },\n" +
//        "    \"scope\": {\n" +
//        "      \"project\": {\n" +
//        "        \"name\": \"cn-north-1\"\n" +
//        "      }\n" +
//        "    }\n" +
//        "  }\n" +
//        "}";
//
//    postAsync("https://iam.cn-north-1.myhuaweicloud.com/v3/auth/tokens", loginRequest,
//        new Callback() {
//          @Override public void onFailure(Call call, IOException e) {
//            e.printStackTrace();
//          }
//
//          @Override public void onResponse(Call call, Response response) throws IOException {
//            if (response.isSuccessful()) {
//
//              ObjectMapper objectMapper = new ObjectMapper();
//              Map<String, Object> jsonNodeMap = null;
//              try {
//                jsonNodeMap = objectMapper
//                    .readValue(response.body().string(), new TypeReference<Map<String, Object>>() {
//                    });
//                if (jsonNodeMap == null) {
//
//                }
//              } catch (Exception e) {
//                e.printStackTrace();
//              }
//
//              LoginInfo loginInfo = new LoginInfo();
//              loginInfo.setUserName(username);
//              loginInfo.setLoggedIn(true);
//              loginInfo.setToken(response.header("X-Subject-Token"));
//              loginInfo.setProjectId(
//                  ((Map) ((Map) jsonNodeMap.get("token")).get("project")).get("id").toString());
//              System.out.println(loginInfo);
//              try {
////                fetchCloudDataSet(loginInfo);
//                createTrainingJob(loginInfo);
//              } catch (ExecutionException e) {
//                e.printStackTrace();
//              } catch (InterruptedException e) {
//                e.printStackTrace();
//              }
//            }
//          }
//        });
//
//  }
//
//  private static MediaType JSON = MediaType.get("application/json; charset=utf-8");
//  private static OkHttpClient client = new OkHttpClient();
//
//  public static void postAsync(String url, String json, Callback callback) {
//    RequestBody body = RequestBody.create(JSON, json);
//    final Request request = new Request.Builder().url(url).post(body).build();
//    Call call = client.newCall(request);
//    call.enqueue(callback);
//  }
//
//  public static void postAsync(String url, String json, String token, Callback callback) {
//    RequestBody body = RequestBody.create(JSON, json);
//    final Request request = new Request.Builder().
//        url(url)
//        .addHeader("X-Auth-Token", token)
//        .post(body).build();
//    Call call = client.newCall(request);
//    call.enqueue(callback);
//  }
//
//  public static Response getSync(String url, String token) throws IOException {
//    final Request request =
//        new Request.Builder().url(url).addHeader("X-Auth-Token", token).get().build();
//    return client.newCall(request).execute();
//  }
//
//  private static void createTrainingJob(LoginInfo loginInfo)
//      throws IOException, ExecutionException, InterruptedException {
//
//    String createTrainJson = "{\"job_name\":\"trainjob-mxnet-mnist1\",\n" + "\"job_desc\":\"\",\n"
//        + "\"config\":\n" + "\t{\"worker_server_num\":1,\n" + "\t\"parameter\":[\n" + "\t\t{\n"
//        + "\t\t\"label\": \"num_epochs\",\n" + "\t\t\"value\": \"10\"\n" + "\t\t}\n" + "\t],\n"
//        + "\t\"pool_id\":\"pool83ec7faf\",\n" + "\t\"train_url\":\"/obs-5b79/mnist-model/\",\n"
//        + "\t\"nas_share_addr\":\"\",\n" + "\t\"nas_mount_path\":\"\",\n"
//        + "\t\"nas_type\":\"nfs\",\n" + "\t\"engine_id\":28,\n"
//        + "\t\"app_url\":\"/obs-5b79/train_mnist/\",\n"
//        + "\t\"boot_file_url\":\"/obs-5b79/train_mnist/train_mnist.py\",\n"
//        + "\t\"dataset_id\":\"a0dbaad1-8e23-423b-a4ee-5ff312f99555\",\n"
//        + "\t\"dataset_version_id\":\"b38e046e-d702-4ebc-9a97-bde777544172\",\n"
//        + "\t\"dataset_name\":\"dataset-mnist(old)\",\n"
//        + "\t\"dataset_version_name\":\"dataset-mnist_V001\",\n"
//        + "\t\"log_url\":\"/obs-5b79/train-log/\"},\n"
//        + "\"notification\":{\"topic_urn\":\"\",\"events\":[]},\"workspace_id\":\"0\"}";
//
//    postAsync("https://modelarts.cn-north-1.myhuaweicloud.com/v1/" + loginInfo.getProjectId()
//        + "/training-jobs", createTrainJson, loginInfo.getToken(),
//        new Callback() {
//          @Override public void onFailure(Call call, IOException e) {
//            e.printStackTrace();
//          }
//
//          @Override public void onResponse(Call call, Response response) throws IOException {
//            if (response.isSuccessful()) {
//
//              ObjectMapper objectMapper = new ObjectMapper();
//              Map<String, Object> jsonNodeMap = null;
//              try {
//                String string = response.body().string();
//                System.out.println(string);
//                jsonNodeMap = objectMapper
//                    .readValue(string, new TypeReference<Map<String, Object>>() {
//                    });
//                if (jsonNodeMap == null) {
//
//                }
//              } catch (Exception e) {
//                e.printStackTrace();
//              }
//
//            } else {
//              System.out.println(response.body().string());
//            }
//          }
//        });
//
//  }
//
//
//  private static void fetchCloudDataSet(LoginInfo loginInfo)
//      throws IOException, ExecutionException, InterruptedException {
//    ExecutorService service = Executors.newSingleThreadExecutor();
//    Future<Response> future = service.submit(() -> getSync(
//        "https://modelarts.cn-north-1.myhuaweicloud.com/v1/" + loginInfo.getProjectId()
//            + "/datasets", loginInfo.getToken()));
//
//    Response response = future.get();
//    if (response.isSuccessful()) {
//      String body = response.body().string();
//      System.out.println(body);
//    }
//    service.shutdown();
//
//  }
//}
