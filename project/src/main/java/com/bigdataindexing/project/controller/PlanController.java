package com.bigdataindexing.project.controller;


import com.bigdataindexing.project.service.AuthorizeService;
import com.bigdataindexing.project.service.PlanService;
import com.bigdataindexing.project.validator.JsonValidator;
import org.everit.json.schema.ValidationException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.json.JSONObject;
import javax.validation.Valid;
import javax.ws.rs.BadRequestException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;


@RestController
public class PlanController {

    PlanService planService;
    AuthorizeService authorizeService;
    JsonValidator jsonValidator;

    public PlanController(PlanService planService, AuthorizeService authorizeService, JsonValidator jsonValidator) {

        this.planService = planService;
        this.authorizeService = authorizeService;
        this.jsonValidator = jsonValidator;

    }


    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/token")
    public ResponseEntity getToken(){

        String token;
        try {
            token = authorizeService.generateToken();
        } catch (Exception e) {
            throw new BadRequestException(e.getMessage());
        }

        return ResponseEntity.status(HttpStatus.CREATED).body(new JSONObject().put("token", token).toString());

    }

    @RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan")
    public ResponseEntity createPlan(@Valid @RequestBody(required = false) String jsonData,
                                     @RequestHeader HttpHeaders requestHeaders) throws URISyntaxException {

        String authorization = requestHeaders.getFirst("Authorization");
        String result = authorizeService.authorize(authorization);
        if(result != "Valid Token"){
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(new JSONObject().put("Error: ", result).toString());
        }

        if (jsonData == null || jsonData.isEmpty()){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).
                    body(new JSONObject().put("error", "Request body is Empty. Kindly provide the JSON").toString());
        }

        JSONObject jsonPlan = new JSONObject(jsonData);

        try {
            jsonValidator.validateJSON(jsonPlan);
        } catch(ValidationException ex){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).
                    body(new JSONObject().put("error",ex.getAllMessages()).toString());
        }

        if(this.planService.checkIfKeyExists(jsonPlan.get("objectType") + ":" + jsonPlan.get("objectId"))){
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(new JSONObject().put("message", "Plan already exists!!").toString());
        }


        String key = jsonPlan.get("objectType") + ":" + jsonPlan.get("objectId");
        String etag = this.planService.savePlan(jsonPlan, key);

        JSONObject response = new JSONObject();
        response.put("objectId", jsonPlan.get("objectId"));
        response.put("message", "Plan Created Successfully!!");

        return ResponseEntity.created(new URI("/plan/" + key)).eTag(etag)
                .body(response.toString());
    }

    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/{objectType}/{objectID}")
    public ResponseEntity getPlan(@PathVariable String objectID, @PathVariable String objectType,
                                        @RequestHeader HttpHeaders requestHeaders){

        String authorization = requestHeaders.getFirst("Authorization");
        String result = authorizeService.authorize(authorization);
        if(result != "Valid Token"){
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(new JSONObject().put("Error: ", result).toString());
        }

        String key = objectType + ":" + objectID;
        if(!this.planService.checkIfKeyExists(key)){
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("message", "ObjectId does not exists!!").toString());
        } else {

            String ifNotMatch;
            try{
                ifNotMatch = requestHeaders.getFirst("If-None-Match");
            } catch (Exception e){
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).
                        body(new JSONObject().put("error", "ETag value is invalid! If-None-Match value should be string.").toString());
            }
            if(objectType.equals("plan")){
                String actualEtag = this.planService.getEtag(key);
                if (ifNotMatch != null && ifNotMatch.equals(actualEtag)) {
                    return ResponseEntity.status(HttpStatus.NOT_MODIFIED).eTag(actualEtag).build();
                }
            }

            Map<String, Object> plan = this.planService.getPlan(key);

            if (objectType.equals("plan")) {
                String actualEtag = this.planService.getEtag(key);
                return ResponseEntity.ok().eTag(actualEtag).body(new JSONObject(plan).toString());
            }

            return ResponseEntity.ok().body(new JSONObject(plan).toString());
        }
    }

    @RequestMapping(method =  RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE, value = "/{objectType}/{objectID}")
    public ResponseEntity deletePlan(@RequestHeader HttpHeaders requestHeaders,
                                        @PathVariable String objectID,  @PathVariable String objectType){

        String authorization = requestHeaders.getFirst("Authorization");
        String result = authorizeService.authorize(authorization);
        if(result != "Valid Token"){
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(new JSONObject().put("Error: ", result).toString());
        }

        String key = objectType + ":" + objectID;
        if(!this.planService.checkIfKeyExists(key)){
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("message", "ObjectId does not exists!!").toString());
        }


        String actualEtag = planService.getEtag(key);
        String eTag = requestHeaders.getFirst("If-Match");
        if (eTag == null || eTag.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new JSONObject().put("message", "eTag not provided in request!!").toString());
        }
        if (eTag != null && !eTag.equals(actualEtag)) {
            return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).eTag(actualEtag)
                    .body(new JSONObject().put("message", "Plan has been updated by another user!!").toString());
        }

        this.planService.deletePlan(key);
        return ResponseEntity.noContent().build();
    }

    @RequestMapping(method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE, path = "/plan/{objectID}")
    public ResponseEntity updatePlan( @RequestHeader HttpHeaders requestHeaders, @Valid @RequestBody(required = false) String jsonData,
                                             @PathVariable String objectID) throws IOException {

        String authorization = requestHeaders.getFirst("Authorization");
        String result = authorizeService.authorize(authorization);
        if(result != "Valid Token"){
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(new JSONObject().put("Error: ", result).toString());
        }

        if (jsonData == null || jsonData.isEmpty()){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).
                    body(new JSONObject().put("error", "Request body is Empty. Kindly provide the JSON").toString());
        }

        JSONObject jsonPlan = new JSONObject(jsonData);
        String key = "plan:" + objectID;

        if(!this.planService.checkIfKeyExists(key)){
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("message", "ObjectId does not exists!!").toString());
        }

        String actualEtag = planService.getEtag(key);
        String eTag = requestHeaders.getFirst("If-Match");
        if (eTag == null || eTag.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new JSONObject().put("message", "eTag not provided in request!!").toString());
        }
        if (eTag != null && !eTag.equals(actualEtag)) {
            return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).eTag(actualEtag)
                    .body(new JSONObject().put("message", "Plan has been updated by another user!!").toString());
        }

        try {
            jsonValidator.validateJSON(jsonPlan);
        } catch(ValidationException ex){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).
                    body(new JSONObject().put("error",ex.getAllMessages()).toString());
        }

        this.planService.deletePlan(key);
        String newEtag = this.planService.savePlan(jsonPlan, key);

        return ResponseEntity.ok().eTag(newEtag)
                .body(new JSONObject().put("message: ", "Resource updated successfully!!").toString());
    }

    @RequestMapping(method =  RequestMethod.PATCH, produces = MediaType.APPLICATION_JSON_VALUE, path = "/plan/{objectID}")
    public ResponseEntity<Object> patchPlan(@RequestHeader HttpHeaders requestHeaders, @Valid @RequestBody(required = false) String jsonData,
                                            @PathVariable String objectID) throws IOException {

        String authorization = requestHeaders.getFirst("Authorization");
        String result = authorizeService.authorize(authorization);
        if(result != "Valid Token"){
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(new JSONObject().put("Error: ", result).toString());
        }

        if (jsonData == null || jsonData.isEmpty()){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).
                    body(new JSONObject().put("error", "Request body is Empty. Kindly provide the JSON").toString());
        }

        JSONObject jsonPlan = new JSONObject(jsonData);
        String key = "plan:" + objectID;
        if (!planService.checkIfKeyExists(key)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("message", "ObjectId does not exists!!").toString());
        }

        String actualEtag = planService.getEtag(key);
        String eTag = requestHeaders.getFirst("If-Match");
        if (eTag == null || eTag.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new JSONObject().put("message", "eTag not provided in request!!").toString());
        }
        if (eTag != null && !eTag.equals(actualEtag)) {
            return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).eTag(actualEtag)
                    .body(new JSONObject().put("message", "Plan has been updated by another user!!").toString());
        }

//        JSONObject mergedPlan = this.planService.mergeData(jsonPlan, key);
//
//        try {
//            jsonValidator.validateJSON(mergedPlan);
//        } catch(ValidationException ex){
//            return ResponseEntity.status(HttpStatus.BAD_REQUEST).
//                    body(new JSONObject().put("error",ex.getAllMessages()).toString());
//        }
//
//        String newEtag =  this.planService.savePlan(mergedPlan, key);


        String newEtag =  this.planService.savePlan(jsonPlan, key);

        return ResponseEntity.ok().eTag(newEtag)
                .body(new JSONObject().put("message: ", "Resource updated successfully!!").toString());
    }

}
