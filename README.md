# test-code

## Note

The Angular component and services in this repository are excerpts from a project. They are provided as code examples, not as working application logic: there is no build setup here (their import files that are not included).

Component:
    `test.component.ts` - a floating panel that tracks CV improvement progress per candidate  and opens the approval dialog;

Services: 
    `test_1.service.ts` (`CommonService`) loads and caches reference data; 
    `test_2.service.ts` (`MatchFeedbackService`) holds CV-improvement state, listens to the WebSocket pipeline, approves improvements and downloads converted CVs;